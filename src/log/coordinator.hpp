#ifndef __LOG_COORDINATOR_HPP__
#define __LOG_COORDINATOR_HPP__

#include <list>
#include <string>

#include <process/process.hpp>

#include "common/result.hpp"

#include "log/replica.hpp"

#include "messages/log.hpp"


namespace mesos { namespace internal { namespace log {

using namespace process;

// TODO(benh): Abstract away the concept of a "group" so that we can
// inject a ZooKeeper based group.

class Coordinator
{
public:
  Coordinator(int quorum,
              ReplicaProcess* replica,
              GroupProcess* group);

  ~Coordinator();

  // Handles coordinator election/demotion. A result of none means the
  // coordinator failed to achieve a quorum (e.g., due to timeout)
  // but can be retried.
  Result<bool> elect(uint64_t id);
  Result<bool> demote();

  // Returns the result of trying to append the specified bytes. A
  // result of none means the append failed (e.g., due to timeout),
  // but can be retried.
  Result<uint64_t> append(const std::string& bytes);

  // Returns the result of trying to truncate the log (from the
  // beginning to the specified position exclusive). A result of
  // none means the truncate failed (e.g., due to timeout), but can be
  // retried.
  Result<uint64_t> truncate(uint64_t to);

  // Returns the result of trying to read entries between from and to,
  // with no-ops and truncates filtered out. A result of none means
  // the read failed (e.g., due to timeout), but can be retried.
  Result<std::list<std::pair<uint64_t, std::string> > > read(
      uint64_t from,
      uint64_t to);

private:
  // Helper that tries to achieve consensus of the specified action. A
  // result of none means the write failed (e.g., due to timeout), but
  // can be retried.
  Result<uint64_t> write(const Action& action);

  // Helper that handles commiting an action.
  Result<uint64_t> commit(const Action& action);
 
  // Helper that tries to fill a position in the log.
  Result<Action> fill(uint64_t position);

  // Helper that uses the specified protocol to broadcast a request to
  // our group and return a set of futures.
  template <typename Req, typename Res>
  std::set<Future<Res> > broadcast(
      const Protocol<Req, Res>& protocol,
      const Req& req);

  // Helper like broadcast, but excludes our local replica.
  template <typename Req, typename Res>
  std::set<Future<Res> > remotecast(
      const Protocol<Req, Res>& protocol,
      const Req& req);

  // Helper like remotecast but ignores any responses.
  template <typename M>
  void remotecast(const M& m);

  bool ready; // True if this coordinator is ready to handle requests.

  int quorum; // Quorum size.

  ReplicaProcess* replica; // Local log replica.

  GroupProcess* group; // Used to broadcast requests and messages to replicas.

  uint64_t id; // Coordinator ID.

  uint64_t index; // Last position written in the log.
};

}}} // namespace mesos { namespace internal { namespace log {

#endif // __LOG_COORDINATOR_HPP__
