#ifndef __LOG_REPLICA_HPP__
#define __LOG_REPLICA_HPP__

#include <algorithm>
#include <set>
#include <string>

#include <process/protobuf.hpp>

#include "common/result.hpp"
#include "common/utils.hpp"

#include "log/cache.hpp"

#include "messages/log.hpp"


namespace mesos { namespace internal { namespace log {

namespace protocol {

// Some replica protocol declarations.
extern Protocol<PromiseRequest, PromiseResponse> promise;
extern Protocol<WriteRequest, WriteResponse> write;
extern Protocol<CommitRequest, CommitResponse> commit;
extern Protocol<LearnRequest, LearnResponse> learn;

} // namespace protocol {

// TODO(benh|jsirois): Integrate ZooKeeper!

class ReplicaProcess : public ProtobufProcess<ReplicaProcess>
{
public:
  // Constructs a new replica process using specified file as the
  // backing store and a cache with the specified capacity.
  ReplicaProcess(const std::string& file, int capacity = 100000);

  virtual ~ReplicaProcess();

  // Handles a request from a coordinator to promise not to accept
  // writes from any other coordinator.
  void promise(const PromiseRequest& request);

  // Handles a request from a coordinator to write an action.
  void write(const WriteRequest& request);

  // Handles a request from a coordinator to commit an action.
  void commit(const CommitRequest& request);

  // Handles a request from a coordinator (or replica) to learn the
  // specified position in the log.
  void learn(uint64_t position);

  // Handles a message notifying of a learned action.
  void learned(const Action& action);

  // Returns the action associated with this position. A none result
  // means that no action is known for this position. An error result
  // means that there was an error while trying to get this action
  // (for example, going to disk to read the log may have
  // failed). Note that reading a position that has been learned to
  // be truncated will also return an error.
  Result<Action> read(uint64_t position);

  // Returns missing positions in the log (i.e., unlearned or holes)
  // up to the specified position.
  std::set<uint64_t> missing(uint64_t position);

private:
  // Helper routines that write a record corresponding to the
  // specified argument. Returns true on success and false otherwise.
  bool persist(const Promise& promise);
  bool persist(const Action& action);

  // Helper routine to recover log state (e.g., on restart).
  void recover();

  // File name for the log.
  const std::string file;

  // File descriptor for the log. Note that this descriptor is used
  // for both reading and writing. This is accomplished because the
  // file gets opend in append only mode, so all writes will naturally
  // move the file offset to the end. Thus, reading is just a matter
  // of seeking to some offset (usually the beginning).
  int fd;

  // Last promised coordinator.
  uint64_t promised;

  // Start position of log (after *learned* truncations).
  uint64_t start;

  // Last position written in the log.
  uint64_t end;

  // Holes in the log.
  std::set<uint64_t> holes;

  // Unlearned positions in the log.
  std::set<uint64_t> unlearned;

  // Cache of log actions (indexed by position).
  cache<uint64_t, Action> cache;
};

}}} // namespace mesos { namespace internal { namespace log {

#endif // __LOG_REPLICA_HPP__
