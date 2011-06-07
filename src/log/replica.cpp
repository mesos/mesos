#include <process/dispatch.hpp>

#include "replica.hpp"

#include "common/utils.hpp"

#include "messages/log.hpp"

using namespace process;

using std::string;


namespace mesos { namespace internal { namespace log {

// Default capacity of actions cache.
const int DEFAULT_CACHE_CAPACITY = 1000;


ReplicaProcess::ReplicaProcess(const string& _file)
  : file(_file),
    fd(-1),
    promised(0),
    index(0),
    cache(DEFAULT_CACHE_CAPACITY)
{
  LOG(INFO) << "Attempting to open log at '" << file << "'";

  Result<int> result =
    utils::os::open(file, O_RDWR | O_CREAT | O_APPEND,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO);

  CHECK(result.isSome()) << "Failed to open log";

  fd = result.get();

  // Recover our last "state" using the log!
  recover();

  // Install protobuf handlers.
  installProtobufHandler<PromiseRequest>(
      &ReplicaProcess::promise,
      &PromiseRequest::id,
      &PromiseRequest::position);

  installProtobufHandler<NopRequest>(
      &ReplicaProcess::nop,
      &NopRequest::id,
      &NopRequest::position);

  installProtobufHandler<AppendRequest>(
      &ReplicaProcess::append,
      &AppendRequest::id,
      &AppendRequest::position,
      &AppendRequest::bytes);

  installProtobufHandler<TruncateRequest>(
      &ReplicaProcess::truncate,
      &TruncateRequest::id,
      &TruncateRequest::position,
      &TruncateRequest::from);

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
// after reaching some threshold aborting.


void ReplicaProcess::promise(uint64_t id, uint64_t position)
{
  if (message<PromiseRequest>().has_position()) {
    // Need to get the action for the specified position.
    Result<Action> result = get(position);

    if (result.isError()) {
      LOG(ERROR) << "Error getting log record at " << position
                 << " : " << result.error();
    } else if (result.isNone()) {
      Action action;
      action.set_position(position);
      action.set_promised(id);

      if (!write(action)) {
        LOG(ERROR) << "Error writing placeholder action to log";
      } else {
        PromiseResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    } else {
      Action action = result.get();

      if (id < action.promised()) {
        PromiseResponse response;
        response.set_okay(false);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      } else {
        Action original = action;
        action.set_promised(id);

        if (!write(action)) {
          LOG(ERROR) << "Error writing updated Action to log";
        } else {
          PromiseResponse response;
          response.set_okay(true);
          response.set_id(id);
          response.mutable_action()->MergeFrom(original);
          send(from(), response);
        }
      }
    }
  } else {
    if (id < promised) {
      PromiseResponse response;
      response.set_okay(false);
      response.set_id(id);
      send(from(), response);
    } else {
      Promise promise;
      promise.set_id(id);

      if (!write(promise)) {
        LOG(ERROR) << "Error writing promise to log";
      } else {
        promised = id;

        // Return the tail position, or the tail action if applicable.
        Result<Action> result = get(index);

        if (result.isError()) {
          LOG(ERROR) << "Error getting log record at " << position
                     << " : " << result.error();
        } else if (result.isNone()) {
          PromiseResponse response;
          response.set_okay(true);
          response.set_id(id);
          response.set_position(position);
          send(from(), response);
        } else if (result.isSome()) {
          PromiseResponse response;
          response.set_okay(true);
          response.set_id(id);
          response.mutable_action()->MergeFrom(result.get());
          send(from(), response);
        }
      }
    }
  }
}


void ReplicaProcess::nop(uint64_t id, uint64_t position)
{
  Result<Action> result = get(position);

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << position
               << " : " << result.error();
  } else if (result.isNone()) {
    if (id < promised) {
      NopResponse response;
      response.set_okay(false);
      response.set_id(id);
      response.set_position(position);
      send(from(), response);
    } else {
      Action action;
      action.set_position(position);
      action.set_promised(promised);
      action.set_performed(id);
      action.set_type(Action::NOP);
      action.mutable_nop();

      if (!write(action)) {
        LOG(ERROR) << "Error writing NOP action to log";
      } else {
        NopResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    }
  } else if (result.isSome()) {
    Action action = result.get();

    if (id < action.promised()) {
      NopResponse response;
      response.set_okay(false);
      response.set_id(id);
      response.set_position(position);
      send(from(), response);
    } else {
      // TODO(benh): Check if this position has already been learned,
      // and if so, check that we are re-writing the same value!
      action.set_performed(id);
      action.clear_learned();
      action.clear_type();
      action.clear_nop();
      action.clear_append();
      action.clear_truncate();
      action.set_type(Action::NOP);
      action.mutable_nop();

      if (!write(action)) {
        LOG(ERROR) << "Error writing NOP action to log";
      } else {
        NopResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    }
  }
}


void ReplicaProcess::append(
    uint64_t id,
    uint64_t position,
    const string& bytes)
{
  Result<Action> result = get(position);

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << position
               << " : " << result.error();
  } else if (result.isNone()) {
    if (id < promised) {
      AppendResponse response;
      response.set_okay(false);
      response.set_id(id);
      response.set_position(position);
      send(from(), response);
    } else {
      Action action;
      action.set_position(position);
      action.set_promised(promised);
      action.set_performed(id);
      action.set_type(Action::APPEND);
      Action::Append* append = action.mutable_append();
      append->set_bytes(bytes);

      if (!write(action)) {
        LOG(ERROR) << "Error writing Append action to log";
      } else {
        AppendResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    }
  } else if (result.isSome()) {
    Action action = result.get();

    if (id < action.promised()) {
      AppendResponse response;
      response.set_okay(false);
      response.set_id(id);
      response.set_position(position);
      send(from(), response);
    } else {
      // TODO(benh): Check if this position has already been learned,
      // and if so, check that we are re-writing the same value!
      action.set_performed(id);
      action.clear_learned();
      action.clear_type();
      action.clear_nop();
      action.clear_append();
      action.clear_truncate();
      action.set_type(Action::APPEND);
      Action::Append* append = action.mutable_append();
      append->set_bytes(bytes);

      if (!write(action)) {
        LOG(ERROR) << "Error writing Append action to log";
      } else {
        AppendResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    }
  }
}


void ReplicaProcess::truncate(uint64_t id, uint64_t position, uint64_t at)
{
  Result<Action> result = get(position);

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << position
               << " : " << result.error();
  } else if (result.isNone()) {
    if (id < promised) {
      TruncateResponse response;
      response.set_okay(false);
      response.set_id(id);
      response.set_position(position);
      send(from(), response);
    } else {
      Action action;
      action.set_position(position);
      action.set_promised(promised);
      action.set_performed(id);
      action.set_type(Action::TRUNCATE);
      Action::Truncate* truncate = action.mutable_truncate();
      truncate->set_at(at);

      if (!write(action)) {
        LOG(ERROR) << "Error writing Truncate action to log";
      } else {
        TruncateResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    }
  } else if (result.isSome()) {
    Action action = result.get();

    if (id < action.promised()) {
      TruncateResponse response;
      response.set_okay(false);
      response.set_id(id);
      response.set_position(position);
      send(from(), response);
    } else {
      // TODO(benh): Check if this position has already been learned,
      // and if so, check that we are re-writing the same value!
      action.set_performed(id);
      action.clear_learned();
      action.clear_type();
      action.clear_nop();
      action.clear_append();
      action.clear_truncate();
      action.set_type(Action::TRUNCATE);
      Action::Truncate* truncate = action.mutable_truncate();
      truncate->set_at(at);

      if (!write(action)) {
        LOG(ERROR) << "Error writing Truncate action to log";
      } else {
        TruncateResponse response;
        response.set_okay(true);
        response.set_id(id);
        response.set_position(position);
        send(from(), response);
      }
    }
  }
}


void ReplicaProcess::learned(const Action& action)
{
  CHECK(action.learned());

  if (!write(action)) {
    LOG(ERROR) << "Error writing learned action to log";
  }
}


void ReplicaProcess::learn(uint64_t position)
{
  Result<Action> result = get(position);

  if (result.isError()) {
    LOG(ERROR) << "Error getting log record at " << position
               << " : " << result.error();
  } else if (result.isNone()) {
    LearnResponse response;
    response.set_okay(false);
    send(from(), response);
  } else if (result.isSome()) {
    LearnResponse response;
    response.set_okay(true);
    response.mutable_action()->MergeFrom(result.get());
    send(from(), response);
  }
}


void ReplicaProcess::recover()
{
  CHECK(fd != -1);

  // Make sure we start at the beginning of the log.
  if (lseek(fd, 0, SEEK_SET) < 0) {
    LOG(FATAL) << "Failed to seek to the beginning of the log";
  }

  Record record;
  Result<bool> result = Result<bool>::none();

  do {
    result = utils::protobuf::read(fd, &record);
    if (result.isError()) {
      LOG(FATAL) << "Failed to read a record from the log: " << result.error();
    } else if (result.isSome()) {
      if (record.type() == Record::PROMISE) {
        promised = record.promise().id();
      } else if (record.type() == Record::ACTION) {
        cache.put(record.action().position(), record.action());
      }
    }
  } while (result.isSome());
}

}}} // namespace mesos { namespace internal { namespace log {
