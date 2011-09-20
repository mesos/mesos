#ifndef __LOG_REPLICA_HPP__
#define __LOG_REPLICA_HPP__

#include <list>
#include <set>
#include <string>

#include <process/process.hpp>
#include <process/protobuf.hpp>

#include "common/result.hpp"
#include "common/try.hpp"

#include "log/cache.hpp"

#include "messages/log.hpp"


namespace mesos {
namespace internal {
namespace log {

namespace protocol {

// Some replica protocol declarations.
extern Protocol<PromiseRequest, PromiseResponse> promise;
extern Protocol<WriteRequest, WriteResponse> write;
extern Protocol<LearnRequest, LearnResponse> learn;

} // namespace protocol {


// Forward declaration.
class ReplicaProcess;


class Replica
{
public:
  // Constructs a new replica process using specified path as the
  // underlying local file for the backing store and a cache with the
  // specified capacity.
  Replica(const std::string& path, int capacity = 100000);
  ~Replica();

  // Returns all the actions between the specified positions, unless
  // those positions are invalid, in which case returns an error.
  process::Future<std::list<Action> > read(uint64_t from, uint64_t to);

  // Returns missing positions in the log (i.e., unlearned or holes)
  // up to the specified position.
  process::Future<std::set<uint64_t> > missing(uint64_t position);

  // Returns the beginning position of the log.
  process::Future<uint64_t> beginning();

  // Returns the last written position in the log.
  process::Future<uint64_t> ending();

  // Returns the highest implicit promise this replica has given.
  process::Future<uint64_t> promised();

  // Returns the PID associated with this replica.
  process::PID<ReplicaProcess> pid();

private:
  ReplicaProcess* process;
};


class ReplicaProcess : public ProtobufProcess<ReplicaProcess>
{
public:
  // Constructs a new replica process using specified path as the
  // underlying local file for the backing store and a cache with the
  // specified capacity.
  ReplicaProcess(const std::string& path, int capacity);

  virtual ~ReplicaProcess();

  // Returns the action associated with this position. A none result
  // means that no action is known for this position. An error result
  // means that there was an error while trying to get this action
  // (for example, going to disk to read the log may have
  // failed). Note that reading a position that has been learned to
  // be truncated will also return an error.
  Result<Action> read(uint64_t position);

  // Returns all the actions between the specified positions, unless
  // those positions are invalid, in which case returns an error.
  process::Promise<std::list<Action> > read(uint64_t from, uint64_t to);

  // Returns missing positions in the log (i.e., unlearned or holes)
  // up to the specified position.
  std::set<uint64_t> missing(uint64_t position);

  // Returns the beginning position of the log.
  uint64_t beginning();

  // Returns the last written position in the log.
  uint64_t ending();

  // Returns the highest implicit promise this replica has given.
  uint64_t promised();

private:
  // Handles a request from a coordinator to promise not to accept
  // writes from any other coordinator.
  void promise(const PromiseRequest& request);

  // Handles a request from a coordinator to write an action.
  void write(const WriteRequest& request);

  // Handles a request from a coordinator (or replica) to learn the
  // specified position in the log.
  void learn(uint64_t position);

  // Handles a message notifying of a learned action.
  void learned(const Action& action);

  // Helper routines that write a record corresponding to the
  // specified argument. Returns true on success and false otherwise.
  bool persist(const Promise& promise);
  bool persist(const Action& action);

  // Helper routine to recover log state (e.g., on restart).
  void recover();

  // File descriptor for the log. Note that this descriptor is used
  // for both reading and writing. This is accomplished because the
  // file gets opend in append only mode, so all writes will naturally
  // move the file offset to the end. Thus, reading is just a matter
  // of seeking to some offset (usually the beginning).
  int fd;

  // Last promise made to a coordinator.
  uint64_t coordinator;

  // Beginning position of log (after *learned* truncations).
  uint64_t begin;

  // Ending position of log (last written position).
  uint64_t end;

  // Holes in the log.
  std::set<uint64_t> holes;

  // Unlearned positions in the log.
  std::set<uint64_t> unlearned;

  // Cache of log actions (indexed by position).
  cache<uint64_t, Action> cache;
};


inline Replica::Replica(const std::string& path, int capacity)
{
  process = new ReplicaProcess(path, capacity);
  process::spawn(process);
}


inline Replica::~Replica()
{
  process::terminate(process);
  process::wait(process);
  delete process;
}


inline process::Future<std::list<Action> > Replica::read(
    uint64_t from,
    uint64_t to)
{
  return process::dispatch(process, &ReplicaProcess::read, from, to);
}


inline process::Future<std::set<uint64_t> > Replica::missing(uint64_t position)
{
  return process::dispatch(process, &ReplicaProcess::missing, position);
}


inline process::Future<uint64_t> Replica::beginning()
{
  return process::dispatch(process, &ReplicaProcess::beginning);
}


inline process::Future<uint64_t> Replica::ending()
{
  return process::dispatch(process, &ReplicaProcess::ending);
}


inline process::Future<uint64_t> Replica::promised()
{
  return process::dispatch(process, &ReplicaProcess::promised);
}

inline process::PID<ReplicaProcess> Replica::pid()
{
  return process->self();
}

} // namespace log {
} // namespace internal {
} // namespace mesos {

#endif // __LOG_REPLICA_HPP__
