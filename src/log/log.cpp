#include <set>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

#include <process/dispatch.hpp>
#include <process/process.hpp>
#include <process/protobuf.hpp>

#include "config/config.hpp"

#include "common/fatal.hpp"
#include "common/hashmap.hpp"
#include "common/logging.hpp"
#include "common/result.hpp"
#include "common/utils.hpp"
#ifndef WITH_ZOOKEEPER
#error "Expecting ZooKeeper for building replicated log!"
#else
#include "common/zookeeper.hpp"
#endif

#include "messages/messages.hpp"

using namespace process;

using std::set;
using std::string;
using std::vector;


namespace mesos { namespace internal { namespace log {

// A single-writer replicated log implementation.  There are two
// components that make up the log, a coordinator and a replica.  The
// coordinator is used for performing distributed appends (and reads),
// while the replica manages the actual log data and can perform
// asynchronous catchup.  ZooKeeper is used to elect a "master"
// coordinator.


// Possible errors from invoking the log.
const string TIMEDOUT = "Request timed out while attempting consensus";
const string DEMOTED = "Coordinator was demoted while attempting consensus";


class Log
{
public:
  Log(const string& file,
      const string& servers,
      const string& znode,
      int quorum);

//   ~Log();

private:
};





class CoordinatorProcess : public ProtobufProcess<CoordinatorProcess>
{
public:
  CoordinatorProcess(const string& file,
                     const string& servers,
                     const string& znode);

  virtual ~CoordinatorProcess();

  // A request from a client to append the specified bytes to the end
  // of the log.
  uint64_t append(const string& bytes);

  // A request from a client to read

  // ZooKeeper events.
  void connected();
  void reconnecting();
  void reconnected();
  void expired();
  void updated(const string& path);

private:
  // Current log position.
  uint64_t position;

  // ZooKeeper bits.
  const string servers;
  const string znode;
  ZooKeeper* zk;
  Watcher* watcher;
  uint64_t sequence;

  // Current set of replicas.
  set<UPID> pids;
};




// // Attempts to reach consensus across a set of replicas for the last
// // position to be appended in the log.
// class PositionConsensusProcess
//   : public ProtobufProcess<PositionConsensusProcess>
// {
// public:
//   PositionConsensusProcess(
//       const set<UPID>& _pids,
//       int _quorum,
//       double _timeout,
//       Result<uint64_t>* _result)
//     : pids(_pids),
//       quorum(_quorum),
//       timeout(_timeout),
//       result(_result) {}

// protected:
//   virtual void operator () ()
//   {
//     foreach (const UPID& pid, pids) {
//       send(pid, PositionRequest());
//     }

//     // Counts of log positions.
//     hashmap<uint64_t, int> counts;

//     double startTime = elapsedTime();

//     while (receive(timeout) != TIMEOUT) {
//       CHECK(name() == PositionResponse().GetTypeName());
//       CHECK(pids.count(from()) > 0);

//       PositionResponse response;
//       response.ParseFromString(body());

//       uint64_t position = response.position();

//       counts[position]++;

//       if (counts[position] >= quorum - 1) {
//         *result = Result<uint64_t>::some(position);
//         return;
//       }

//       timeout = timeout - (elapsedTime() - startTime);
//     }
//   }

// private:
//   set<UPID> pids;
//   int quorum;
//   double timeout;
//   Result<uint64_t>* result;
// };


// Result<uint64_t> getPositionConsensus(
//     const set<UPID>& pids,
//     int quorum,
//     double timeout)
// {
//   Result<uint64_t> result = Result<uint64_t>::none();
//   PositionConsensusProcess process(pids, quorum, timeout, &result);
//   wait(spawn(process));
//   return result;
// }


Log::Log(const string& file,
         const string& servers,
         const string& znode,
         int quorum) {}


Log::~Log() throw () {}

}}} // namespace mesos { namespace internal { namespace log {


using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::log;


int main(int argc, char** argv)
{
  if (argc != 5) {
    fatal("usage: %s <file> <servers> <znode> <quorum>", argv[0]);
  }

  string file = argv[1];
  string servers = argv[2];
  string znode = argv[3];
  int quorum = atoi(argv[4]);

  ReplicaProcess replica(file, servers, znode);

  wait(spawn(replica));

  return 0;
}

