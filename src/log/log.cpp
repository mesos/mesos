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


class ReplicaProcess : public ProtobufProcess<ReplicaProcess>
{
public:
  ReplicaProcess(const string& file,
                 const string& servers,
                 const string& znode);

  virtual ~ReplicaProcess();

  // A request from a coordinator to promise not to accept appends
  // from any coordinator with a lower id.
  void promise(uint64_t id, uint64_t position);

  // A request from a coordinator to append the specified bytes to the
  // end of the log.
  void append(uint64_t id, uint64_t position, const string& bytes);

  // A message that signals that a value has been learned.
  void learn(uint64_t id, uint64_t position, const string& bytes);

  // ZooKeeper events.
  void connected();
  void reconnecting();
  void reconnected();
  void expired();
  void updated(const string& path);

private:
  // File info for the log.
  const string file;
  int fd;

  // Last promised coordinator.
  uint64_t id;

  // Last position written in the log.
  uint64_t position;

  // ZooKeeper bits.
  const string servers;
  const string znode;
  ZooKeeper* zk;
  Watcher* watcher;
  uint64_t sequence;
};


// A watcher that just sends events to the replicated log.
class ReplicaProcessWatcher : public Watcher
{
public:
  ReplicaProcessWatcher(const PID<ReplicaProcess>& _pid)
    : pid(_pid), reconnect(false) {}

  virtual ~ReplicaProcessWatcher() {}

  virtual void process(ZooKeeper* zk, int type, int state, const string& path)
  {
    if ((state == ZOO_CONNECTED_STATE) && (type == ZOO_SESSION_EVENT)) {
      // Check if this is a reconnect.
      if (!reconnect) {
        // Initial connect.
        dispatch(pid, &ReplicaProcess::connected);
      } else {
        // Reconnected.
        dispatch(pid, &ReplicaProcess::reconnected);
      }
    } else if ((state == ZOO_CONNECTING_STATE) &&
               (type == ZOO_SESSION_EVENT)) {
      // The client library automatically reconnects, taking into
      // account failed servers in the connection string,
      // appropriately handling the "herd effect", etc.
      reconnect = true;
      dispatch(pid, &ReplicaProcess::reconnecting);
    } else if ((state == ZOO_EXPIRED_SESSION_STATE) &&
               (type == ZOO_SESSION_EVENT)) {
      dispatch(pid, &ReplicaProcess::expired);

      // If this watcher is reused, the next connect won't be a reconnect.
      reconnect = false;
    } else if ((state == ZOO_CONNECTED_STATE) &&
               (type == ZOO_CHILD_EVENT)) {
      dispatch(pid, &ReplicaProcess::updated, path);
    } else if ((state == ZOO_CONNECTED_STATE) &&
               (type == ZOO_CHANGED_EVENT)) {
      dispatch(pid, &ReplicaProcess::updated, path);
    } else {
      LOG(FATAL) << "Unimplemented ZooKeeper event: (state is "
                 << state << " and type is " << type << ")";
    }
  }

private:
  const PID<ReplicaProcess> pid;
  bool reconnect;
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


ReplicaProcess::ReplicaProcess(
    const string& _file,
    const string& _servers,
    const string& _znode)
  : file(_file),
    servers(_servers),
    znode(_znode)
{
  id = 0;
  position = 0;

  watcher = new ReplicaProcessWatcher(self());
  zk = new ZooKeeper(servers, 10000, watcher);

  // Install protobuf handlers.
  installProtobufHandler<PromiseRequest>(
      &ReplicaProcess::promise,
      &PromiseRequest::id,
      &PromiseRequest::position);

  installProtobufHandler<AppendRequest>(
      &ReplicaProcess::append,
      &AppendRequest::id,
      &AppendRequest::position,
      &AppendRequest::bytes);

  installProtobufHandler<LearnMessage>(
      &ReplicaProcess::learn,
      &LearnMessage::id,
      &LearnMessage::position,
      &LearnMessage::bytes);
}


ReplicaProcess::~ReplicaProcess()
{
  CHECK(zk != NULL);
  delete zk;

  CHECK(watcher != NULL);
  delete watcher;
}


void ReplicaProcess::promise(uint64_t id, uint64_t position)
{
  LOG(FATAL) << "unimplemented";
}


void ReplicaProcess::append(
    uint64_t id,
    uint64_t position,
    const string& bytes)
{
  LOG(FATAL) << "unimplemented";
}


void ReplicaProcess::learn(
    uint64_t id,
    uint64_t position,
    const string& bytes)
{
  LOG(FATAL) << "unimplemented";
}


void ReplicaProcess::connected()
{
  LOG(INFO) << "Log replica connected to ZooKeeper";

  int ret;
  string result;

  // Assume the znode that was created does not end with a "/".
  CHECK(znode.size() == 0 || znode.at(znode.size() - 1) != '/');

  // Create directory path znodes as necessary.
  size_t index = znode.find("/", 0);

  while (index < string::npos) {
    // Get out the prefix to create.
    index = znode.find("/", index + 1);
    string prefix = znode.substr(0, index);

    LOG(INFO) << "Log replica trying to create znode '"
              << prefix << "' in ZooKeeper";

    // Create the node (even if it already exists).
    ret = zk->create(prefix, "", ZOO_OPEN_ACL_UNSAFE,
		     // ZOO_CREATOR_ALL_ACL, // needs authentication
		     0, &result);

    if (ret != ZOK && ret != ZNODEEXISTS) {
      LOG(FATAL) << "Failed to create '" << prefix
                 << "' in ZooKeeper: " << zk->error(ret);
    }
  }

  // Create a new ephemeral znode for ourselves and populate it with
  // the pid for this process.
  ret = zk->create(znode + "/", self(), ZOO_OPEN_ACL_UNSAFE,
                   // ZOO_CREATOR_ALL_ACL, // needs authentication
                   ZOO_SEQUENCE | ZOO_EPHEMERAL, &result);

  if (ret != ZOK) {
    LOG(FATAL) << "Failed to create an ephmeral node at '"
               << znode << "' in ZooKeeper: " << zk->error(ret);
  }

  // Save the sequence id but only grab the basename, e.g.,
  // "/path/to/znode/000000131" => "000000131".
  result = utils::os::basename(result);

  try {
    sequence = boost::lexical_cast<uint64_t>(result);
  } catch (boost::bad_lexical_cast&) {
    LOG(FATAL) << "Failed to convert '" << result << "' into an integer";
  }
}


void ReplicaProcess::reconnecting()
{
  LOG(INFO) << "Log replica reconnecting to ZooKeeper";
}


void ReplicaProcess::reconnected()
{
  LOG(INFO) << "Log replica reconnected to ZooKeeper";
}


void ReplicaProcess::expired()
{
  LOG(FATAL) << "Log replica ZooKeeper session expired!";
}


void ReplicaProcess::updated(const string& path)
{
  LOG(FATAL) << "Log replica not expecting updates at '"
             << path << "' in ZooKeeper";
}

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

