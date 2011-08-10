#include <process/dispatch.hpp>

#include "replica.hpp"

#include "common/utils.hpp"

#include "messages/log.hpp"

using namespace process;

using std::set;
using std::string;


namespace mesos { namespace internal { namespace log {

namespace protocol {

// Some replica protocol definitions.
Protocol<PromiseRequest, PromiseResponse> promise;
Protocol<WriteRequest, WriteResponse> write;
Protocol<CommitRequest, CommitResponse> commit;
Protocol<LearnRequest, LearnResponse> learn;

} // namespace protocol {


ReplicaProcess::ReplicaProcess(const string& _path, int capacity)
  : path(_path),
    fd(-1),
    promised(0),
    begin(0),
    end(0),
    cache(capacity)
{
  LOG(INFO) << "Attempting to open log at '" << path << "'";

  Result<int> result =
    utils::os::open(path, O_RDWR | O_CREAT | O_APPEND,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO);

  CHECK(result.isSome()) << "Failed to open log";

  fd = result.get();

  // Recover our last "state" using the log!
  recover();

  // Install protobuf handlers.
  installProtobufHandler<PromiseRequest>(
      &ReplicaProcess::promise);

  installProtobufHandler<WriteRequest>(
      &ReplicaProcess::write);

  installProtobufHandler<CommitRequest>(
      &ReplicaProcess::commit);

  installProtobufHandler<LearnedMessage>(
      &ReplicaProcess::learned,
      &LearnedMessage::action);

  installProtobufHandler<LearnRequest>(
      &ReplicaProcess::learn,
      &LearnRequest::position);
}


ReplicaProcess::~ReplicaProcess()
{
  utils::os::close(fd);
}


// Note that certain failures that occur result in returning from the
// current function but *NOT* sending a 'nack' back to the coordinator
// because that implies a coordinator has been demoted. Not sending
// anything is equivalent to pretending like the request never made it
// here. TODO(benh): At some point, however, we might want to actually
// "fail" more dramatically because there could be something rather
// seriously wrong on this box that we are ignoring (like a bad
// disk). This could be accomplished by changing most LOG(ERROR)
// statements to LOG(FATAL), or by counting the number of errors and
// after reaching some threshold aborting. In addition, sending the
// error information back to the coordinator "might" help the
// debugging procedure.


void ReplicaProcess::promise(const PromiseRequest& request)
{
  if (request.has_position()) {
    LOG(INFO) << "Replica received explicit promise request from"
              << " coordinator " << request.id()
              << " for position " << request.position();

    // Need to get the action for the specified position.
    Result<Action> result = read(request.position());

    if (result.isError()) {
      LOG(ERROR) << "Error getting log record at " << request.position()
                 << " : " << result.error();
    } else if (result.isNone()) {
      Action action;
      action.set_position(request.position());
      action.set_promised(request.id());

      if (!persist(action)) {
        LOG(ERROR) << "Error persisting action to log";
      } else {
        PromiseResponse response;
        response.set_okay(true);
        response.set_id(request.id());
        response.set_position(request.position());
        send(from(), response);
      }
    } else {
      CHECK(result.isSome());
      Action action = result.get();
      CHECK(action.position() == request.position());

      if (request.id() < action.promised()) {
        PromiseResponse response;
        response.set_okay(false);
        response.set_id(request.id());
        response.set_position(request.position());
        send(from(), response);
      } else {
        Action original = action;
        action.set_promised(request.id());

        if (!persist(action)) {
          LOG(ERROR) << "Error persisting action to log";
        } else {
          PromiseResponse response;
          response.set_okay(true);
          response.set_id(request.id());
          response.mutable_action()->MergeFrom(original);
          send(from(), response);
        }
      }
    }
  } else {
    LOG(INFO) << "Replica received implicit promise request from"
              << " coordinator " << request.id();

    if (request.id() < promised) {
      PromiseResponse response;
      response.set_okay(false);
      response.set_id(request.id());
      send(from(), response);
    } else {
      Promise promise;
      promise.set_id(request.id());

      if (!persist(promise)) {
        LOG(ERROR) << "Error persisting promise to log";
      } else {
        promised = request.id();

        // Return the last position written.
        PromiseResponse response;
        response.set_okay(true);
        response.set_id(request.id());
        response.set_position(end);
        send(from(), response);
      }
    }
  }
}


void ReplicaProcess::write(const WriteRequest& request)
{
  Result<Action> result = read(request.position());

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << request.position()
               << " : " << result.error();
  } else if (result.isNone()) {
    if (request.id() < promised) {
      WriteResponse response;
      response.set_okay(false);
      response.set_id(request.id());
      response.set_position(request.position());
      send(from(), response);
    } else {
      Action action;
      action.set_position(request.position());
      action.set_promised(promised);
      action.set_performed(request.id());
      action.set_type(request.type());

      switch (request.type()) {
        case Action::NOP:
          CHECK(request.has_nop());
          action.mutable_nop();
          break;
        case Action::APPEND:
          CHECK(request.has_append());
          action.mutable_append()->MergeFrom(request.append());
          break;
        case Action::TRUNCATE:
          CHECK(request.has_truncate());
          action.mutable_truncate()->MergeFrom(request.truncate());
          break;
        default:
          LOG(FATAL) << "Unknown Action::Type!";
      }

      if (!persist(action)) {
        LOG(ERROR) << "Error persisting action to log";
      } else {
        WriteResponse response;
        response.set_okay(true);
        response.set_id(request.id());
        response.set_position(request.position());
        send(from(), response);
      }
    }
  } else if (result.isSome()) {
    Action action = result.get();
    CHECK(action.position() == request.position());

    if (request.id() < action.promised()) {
      WriteResponse response;
      response.set_okay(false);
      response.set_id(request.id());
      response.set_position(request.position());
      send(from(), response);
    } else {
      // TODO(benh): Check if this position has already been learned,
      // and if so, check that we are re-writing the same value!
      action.set_performed(request.id());
      action.clear_learned();
      action.clear_type();
      action.clear_nop();
      action.clear_append();
      action.clear_truncate();
      action.set_type(request.type());

      switch (request.type()) {
        case Action::NOP:
          CHECK(request.has_nop());
          action.mutable_nop();
          break;
        case Action::APPEND:
          CHECK(request.has_append());
          action.mutable_append()->MergeFrom(request.append());
          break;
        case Action::TRUNCATE:
          CHECK(request.has_truncate());
          action.mutable_truncate()->MergeFrom(request.truncate());
          break;
        default:
          LOG(FATAL) << "Unknown Action::Type!";
      }

      if (!persist(action)) {
        LOG(ERROR) << "Error persisting action to log";
      } else {
        WriteResponse response;
        response.set_okay(true);
        response.set_id(request.id());
        response.set_position(request.position());
        send(from(), response);
      }
    }
  }
}


void ReplicaProcess::commit(const CommitRequest& request)
{
  Result<Action> result = read(request.position());

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << request.position()
               << " : " << result.error();
  } else if (result.isNone()) {
    if (request.id() < promised) {
      CommitResponse response;
      response.set_okay(false);
      response.set_id(request.id());
      response.set_position(request.position());
      send(from(), response);
    } else {
      Action action;
      action.set_position(request.position());
      action.set_promised(promised);
      action.set_performed(request.id());
      action.set_learned(true);
      action.set_type(request.type());

      switch (request.type()) {
        case Action::NOP:
          CHECK(request.has_nop());
          action.mutable_nop();
          break;
        case Action::APPEND:
          CHECK(request.has_append());
          action.mutable_append()->MergeFrom(request.append());
          break;
        case Action::TRUNCATE:
          CHECK(request.has_truncate());
          action.mutable_truncate()->MergeFrom(request.truncate());
          break;
        default:
          LOG(FATAL) << "Unknown Action::Type!";
      }

      if (!persist(action)) {
        LOG(ERROR) << "Error persisting action to log";
      } else {
        CommitResponse response;
        response.set_okay(true);
        response.set_id(request.id());
        response.set_position(request.position());
        send(from(), response);
      }
    }
  } else if (result.isSome()) {
    Action action = result.get();

    if (request.id() < action.promised()) {
      CommitResponse response;
      response.set_okay(false);
      response.set_id(request.id());
      response.set_position(request.position());
      send(from(), response);
    } else {
      // TODO(benh): Check if this position has already been learned,
      // and if so, check that we are re-writing the same value!
      action.set_performed(request.id());
      action.set_learned(true);
      action.clear_type();
      action.clear_nop();
      action.clear_append();
      action.clear_truncate();
      action.set_type(request.type());

      switch (request.type()) {
        case Action::NOP:
          CHECK(request.has_nop());
          action.mutable_nop();
          break;
        case Action::APPEND:
          CHECK(request.has_append());
          action.mutable_append()->MergeFrom(request.append());
          break;
        case Action::TRUNCATE:
          CHECK(request.has_truncate());
          action.mutable_truncate()->MergeFrom(request.truncate());
          break;
        default:
          LOG(FATAL) << "Unknown Action::Type!";
      }

      if (!persist(action)) {
        LOG(ERROR) << "Error persisting action to log";
      } else {
        CommitResponse response;
        response.set_okay(true);
        response.set_id(request.id());
        response.set_position(request.position());
        send(from(), response);
      }
    }
  }
}


void ReplicaProcess::learned(const Action& action)
{
  CHECK(action.learned());

  if (!persist(action)) {
    LOG(ERROR) << "Error persisting action to log";
  }

  LOG(INFO) << "Replica learned "
            << Action::Type_Name(action.type())
            << " action at position " << action.position();
}


void ReplicaProcess::learn(uint64_t position)
{
  Result<Action> result = read(position);

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << position
               << " : " << result.error();
  } else if (result.isSome() &&
             result.get().has_learned() &&
             result.get().learned()) {
    LearnResponse response;
    response.set_okay(true);
    response.mutable_action()->MergeFrom(result.get());
    send(from(), response);
  } else {
    LearnResponse response;
    response.set_okay(false);
    send(from(), response);
  }
}


Result<Action> ReplicaProcess::read(uint64_t position)
{
  if (position == 0) { // TODO(benh): Remove this hack.
    return Result<Action>::none();
  } else if (position < begin) {
    return Result<Action>::error("Attempted to read truncated position");
  } else if (end < position) {
    return Result<Action>::none();
  } else if (holes.count(position) > 0) {
    return Result<Action>::none();
  }

  Option<Action> option = cache.get(position);

  if (option.isSome()) {
    return option.get();
  } else {
    CHECK(fd != -1);

    // TODO(benh): Is there a more efficient way to look up data in
    // the log? At the point we start implementing something like this
    // it will probably be time to move to leveldb, or at least
    // completely abstract this bit outside of the replcia.

    // Make sure we start at the beginning of the log.
    if (lseek(fd, 0, SEEK_SET) < 0) {
      LOG(FATAL) << "Failed to seek to the beginning of the log";
    }

    Result<bool> result = Result<bool>::none();

    do {
      Record record;
      result = utils::protobuf::read(fd, &record);
      if (result.isError()) {
        return Result<Action>::error(result.error());
      } else if (result.isSome()) {
        // A result of 'false' here should not be possible since any
        // inconsistencies in the file should have been taken care of
        // during recovery by doing a file truncate.
        CHECK(result.get());
        if (record.type() == Record::ACTION) {
          CHECK(record.has_action());
          if (record.action().position() == position) {
            // Cache this action, even though as we keep reading through
            // the file we may find a more recently written one.
            cache.put(position, record.action());
          }
        }
      }
    } while (result.isSome());

    // Must have reached EOF.
    CHECK(result.isNone());

    // Okay, the action *must* be in the cache. If it's not that means
    // that we aren't recording our holes correctly.
    option = cache.get(position);
    CHECK(option.isSome());
    return option.get();
  }
}


set<uint64_t> ReplicaProcess::missing(uint64_t index)
{
  // Start off with all the unlearned positions.
  set<uint64_t> positions = unlearned;

  // Add in a spoonful of holes.
  foreach (uint64_t hole, holes) {
    positions.insert(hole);
  }

  // And finally add all the unknown positions beyond our end.
  for (; index >= end; index--) {
    positions.insert(index);

    // Don't wrap around 0!
    if (index == 0) {
      break;
    }
  }

  return positions;
}


Result<uint64_t> ReplicaProcess::beginning()
{
  return begin;
}


Result<uint64_t> ReplicaProcess::ending()
{
  return end;
}


bool ReplicaProcess::persist(const Promise& promise)
{
  Record record;
  record.set_type(Record::PROMISE);
  record.mutable_promise()->MergeFrom(promise);

  Result<bool> result = utils::protobuf::write(fd, record);

  if (result.isError()) {
    LOG(ERROR) << "Error writing to log: " << result.error();
    return false;
  }

  CHECK(result.isSome());

  return result.get();
}


bool ReplicaProcess::persist(const Action& action)
{
  Record record;
  record.set_type(Record::ACTION);
  record.mutable_action()->MergeFrom(action);

  Result<bool> result = utils::protobuf::write(fd, record);

  if (result.isError()) {
    LOG(ERROR) << "Error writing to log: " << result.error();
    return false;
  }

  CHECK(result.isSome());

  if (result.get()) {
    // No longer a hole here (if there even was one).
    holes.erase(action.position());

    // Update unlearned positions and deal with truncation actions.
    if (action.has_learned() && action.learned()) {
      unlearned.erase(action.position());
      if (action.has_type() && action.type() == Action::TRUNCATE) {
        begin = std::max(begin, action.truncate().to());
      }
    }

    // Update holes if we just wrote many positions past the last end.
    for (uint64_t position = end + 1; position < action.position(); position++) {
      holes.insert(position);
    }

    end = std::max(end, action.position());
    cache.put(action.position(), action);
    return true;
  }

  return false;
}


void ReplicaProcess::recover()
{
  CHECK(fd != -1);

  // Make sure we start at the beginning of the log.
  if (lseek(fd, 0, SEEK_SET) < 0) {
    LOG(FATAL) << "Failed to seek to the beginning of the log";
  }

  // Save the set of learned positions as we recover (we also save the
  // set of unlearned positions beyond the scope of the recover) so
  // that we can determine the holes (i.e., !learned && !unlearned).
  set<uint64_t> learned;

  Result<bool> result = Result<bool>::none();

  do {
    Record record;
    result = utils::protobuf::read(fd, &record);
    if (result.isSome()) {
      if (result.get()) {
        if (record.type() == Record::PROMISE) {
          CHECK(record.has_promise());
          promised = record.promise().id();
        } else {
          CHECK(record.type() == Record::ACTION);
          CHECK(record.has_action());
          if (record.action().has_learned() && record.action().learned()) {
            learned.insert(record.action().position());
            unlearned.erase(record.action().position());
            if (record.action().has_type() &&
                record.action().type() == Action::TRUNCATE) {
              begin = std::max(begin, record.action().truncate().to());
            }
          } else {
            learned.erase(record.action().position());
            unlearned.insert(record.action().position());
          }
          end = std::max(end, record.action().position());
          cache.put(record.action().position(), record.action());
        }
      } else if (result.isError()) {
        // We might have crashed when trying to write a record or a
        // record got corrupted some other way. Just truncate after
        // this point rather than doing any fancy heuristical recovery
        // and assume if this *is* a non-recoverable file error it
        // will be uncovered when we do the truncate (or possibly
        // later when we actually do a write).
        LOG(WARNING) << "Failed to completely recover the log: "
                     << result.error();

        if (ftruncate(fd, lseek(fd, 0, SEEK_CUR)) != 0) {
          LOG(FATAL) << "Failed to truncate during recovery";
        }

        break;
      }
    }
  } while (result.isSome());

  // Determine the holes.
  for (uint64_t position = begin; position < end; position++) {
    if (learned.count(position) == 0 && unlearned.count(position) == 0) {
      holes.insert(position);
    }
  }

  LOG(INFO) << "Replica recovered with log positions "
            << begin << " -> " << end
            << " and holes " << utils::stringify(holes)
            << " and unlearned " << utils::stringify(unlearned);
}

}}} // namespace mesos { namespace internal { namespace log {
