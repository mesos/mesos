#ifndef __LOG_REPLICA_HPP__
#define __LOG_REPLICA_HPP__

#include <algorithm>
#include <string>

#include <process/protobuf.hpp>

#include "common/result.hpp"
#include "common/utils.hpp"

#include "log/cache.hpp"

#include "messages/log.hpp"


namespace mesos { namespace internal { namespace log {

// TODO(benh|jsirois): Integrate ZooKeeper!

class ReplicaProcess : public ProtobufProcess<ReplicaProcess>
{
public:
  ReplicaProcess(const std::string& file);

  virtual ~ReplicaProcess();

  // Handles a request from a coordinator to promise not to accept
  // writes from any coordinator with a lower id.
  void promise(uint64_t id, uint64_t position);

  // Handles a request to insert a nop in the log.
  void nop(uint64_t id, uint64_t position);

  // Handles a request to append to the log.
  void append(uint64_t id, uint64_t position, const std::string& bytes);

  // Handles a request to truncate the log.
  void truncate(uint64_t id, uint64_t position, uint64_t at);

  // Handles a message notifying of a learned action.
  void learned(const Action& action);

  // Handles a request to learn the specified position in the log.
  void learn(uint64_t position);

private:
  // Returns the action associated with this position. A result of
  // type Result::NONE means that no action is known for this
  // position. A result of type Result::ERROR means that there was an
  // error while trying to get this action (for example, going to disk
  // to read the log may have failed).
  Result<Action> get(uint64_t position);

  // Helper routines that write a record corresponding to the
  // specified argument. Returns true on success and false otherwise.
  bool write(const Promise& promise);
  bool write(const Action& action);

  // Helper routine to recover log state (e.g., on restart).
  void recover();

  // File info for the log.
  const std::string file;
  int fd;

  // Last promised coordinator.
  uint64_t promised;

  // Last position written in the log.
  uint64_t index;

  // Cache of log actions (indexed by position).
  cache<uint64_t, Action> cache;
};


inline Result<Action> ReplicaProcess::get(uint64_t position)
{
  Option<Action> option = cache.get(position);

  if (option.isSome()) {
    return option.get();
  } else {
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
        return Result<Action>::error(result.error());
      } else if (result.isNone()) {
        return Result<Action>::none();
      } else {
        CHECK(result.isSome());
        if (record.type() == Record::ACTION &&
            record.action().position() == position) {
          cache.put(position, record.action());
          return record.action();
        }
      }
    } while (result.isSome());

    LOG(FATAL) << "Failed to find action in file OR reach EOF?";
  }
}


inline bool ReplicaProcess::write(const Promise& promise)
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


inline bool ReplicaProcess::write(const Action& action)
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
    index = std::max(index, action.position());
    cache.put(index, action);
    return true;
  }

  return false;
}

}}} // namespace mesos { namespace internal { namespace log {

#endif // __LOG_REPLICA_HPP__
