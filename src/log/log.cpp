#include <set>
#include <string>
#include <vector>

#include <boost/lexical_cast.hpp>

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

using std::set;
using std::string;
using std::vector;

using process::PID;
using process::UPID;
using process::Future;


namespace mesos { namespace internal {

// A single-writer replicated log implementation.  Each instance of a
// ReplicatedLog acts as a replica.  ZooKeeper is used to elect a
// master/coordinator replica.  A master/coordinator replica only
// commits locally after at least a quorum of other replicas have
// committed.  When a replica is elected master/coordinator (e.g.,
// after a master fails) it guarantess consistency by reaching
// consensus on the latest log position written before proceeding.  A
// replica can catchup from other running replicas via a "pull" based
// request.  In addition, a replica that negatively acknowledges an
// append request can receive "push" catchup messages from the
// master/coordinator.

class ReplicatedLog
{
public:
  ReplicatedLog(
      const string& file,
      const string& servers,
      const string& znode,
      int replicas);

  virtual ~ReplicatedLog();
};


class ReplicatedLogProcess : public ProtobufProcess<ReplicatedLogProcess>
{
public:
  ReplicatedLogProcess(
      const string& file,
      const string& servers,
      const string& znode,
      int replicas);

  virtual ~ReplicatedLogProcess();

  // Appends the specified bytes to the end of the log.
  void appendRequest(uint64_t position, const string& bytes);

  // Sends back the current log position.
  void positionRequest();

  // Truncates the log from the specified position forward.
  void truncationRequest(uint64_t position);

  // ZooKeeper events.
  void connected();
  void reconnecting();
  void reconnected();
  void expired();
  void updated(const string& path);

private:
  // File info for the log.
  string file;
  int fd;

  // Local position in the log.
  uint64_t position;

  // ZooKeeper bits and pieces.
  string servers;
  string znode;
  ZooKeeper* zk;
  Watcher* watcher;

  // Our ephemeral sequence number (via ZooKeeper).
  uint64_t sequence;

  // Replica information.
  bool master;
  int replicas;

  // Set of log replicas.
  set<UPID> pids;
};


// A watcher that just sends events to the replicated log.
class ReplicatedLogProcessWatcher : public Watcher
{
public:
  ReplicatedLogProcessWatcher(const PID<ReplicatedLogProcess>& _pid)
    : pid(_pid), reconnect(false) {}

  virtual ~ReplicatedLogProcessWatcher() {}

  virtual void process(ZooKeeper* zk, int type, int state, const string& path)
  {
    if ((state == ZOO_CONNECTED_STATE) && (type == ZOO_SESSION_EVENT)) {
      // Check if this is a reconnect.
      if (!reconnect) {
        // Initial connect.
        process::dispatch(pid, &ReplicatedLogProcess::connected);
      } else {
        // Reconnected.
        process::dispatch(pid, &ReplicatedLogProcess::reconnected);
      }
    } else if ((state == ZOO_CONNECTING_STATE) &&
               (type == ZOO_SESSION_EVENT)) {
      // The client library automatically reconnects, taking into
      // account failed servers in the connection string,
      // appropriately handling the "herd effect", etc.
      reconnect = true;
      process::dispatch(pid, &ReplicatedLogProcess::reconnecting);
    } else if ((state == ZOO_EXPIRED_SESSION_STATE) &&
               (type == ZOO_SESSION_EVENT)) {
      process::dispatch(pid, &ReplicatedLogProcess::expired);

      // If this watcher is reused, the next connect won't be a reconnect.
      reconnect = false;
    } else if ((state == ZOO_CONNECTED_STATE) &&
               (type == ZOO_CHILD_EVENT)) {
      process::dispatch(pid, &ReplicatedLogProcess::updated, path);
    } else if ((state == ZOO_CONNECTED_STATE) &&
               (type == ZOO_CHANGED_EVENT)) {
      process::dispatch(pid, &ReplicatedLogProcess::updated, path);
    } else {
      LOG(FATAL) << "Unimplemented ZooKeeper event: (state is "
                 << state << " and type is " << type << ")";
    }
  }

private:
  const PID<ReplicatedLogProcess> pid;
  bool reconnect;
};



// Attempts to reach consensus across a set of replicas for the last
// position to be appended in the log.
class PositionConsensusProcess
  : public ProtobufProcess<PositionConsensusProcess>
{
public:
  PositionConsensusProcess(
      const set<UPID>& _pids,
      int _quorum,
      double _timeout,
      Result<uint64_t>* _result)
    : pids(_pids),
      quorum(_quorum),
      timeout(_timeout),
      result(_result) {}

protected:
  virtual void operator () ()
  {
    foreach (const UPID& pid, pids) {
      send(pid, PositionRequest());
    }

    // Counts of log positions.
    hashmap<uint64_t, int> counts;

    double startTime = elapsedTime();

    while (receive(timeout) != process::TIMEOUT) {
      CHECK(name() == PositionResponse().GetTypeName());
      CHECK(pids.count(from()) > 0);

      PositionResponse response;
      response.ParseFromString(body());

      uint64_t position = response.position();

      counts[position]++;

      if (counts[position] >= quorum - 1) {
        *result = Result<uint64_t>::some(position);
        return;
      }

      timeout = timeout - (elapsedTime() - startTime);
    }
  }

private:
  set<UPID> pids;
  int quorum;
  double timeout;
  Result<uint64_t>* result;
};


Result<uint64_t> getPositionConsensus(
    const set<UPID>& pids,
    int quorum,
    double timeout)
{
  Result<uint64_t> result = Result<uint64_t>::none();
  PositionConsensusProcess process(pids, quorum, timeout, &result);
  process::wait(process::spawn(&process));
  return result;
}


ReplicatedLog::ReplicatedLog(
    const string& file,
    const string& servers,
    const string& znode,
    int replicas) {}


ReplicatedLog::~ReplicatedLog() {}


ReplicatedLogProcess::ReplicatedLogProcess(
    const string& _file,
    const string& _servers,
    const string& _znode,
    int _replicas)
  : file(_file),
    servers(_servers),
    znode(_znode),
    replicas(_replicas)
{
  position = 0;
  master = false;

  watcher = new ReplicatedLogProcessWatcher(self());
  zk = new ZooKeeper(servers, 10000, watcher);

  // Install protobuf handlers.
  installProtobufHandler<AppendRequest>(
      &ReplicatedLogProcess::appendRequest,
      &AppendRequest::position,
      &AppendRequest::bytes);

  installProtobufHandler<PositionRequest>(
      &ReplicatedLogProcess::positionRequest);
}


ReplicatedLogProcess::~ReplicatedLogProcess()
{
  CHECK(zk != NULL);
  delete zk;

  CHECK(watcher != NULL);
  delete watcher;
}


void ReplicatedLogProcess::appendRequest(
    uint64_t position,
    const string& bytes)
{
  LOG(FATAL) << "unimplemented";
}


void ReplicatedLogProcess::positionRequest()
{
  LOG(INFO) << "Log position requested";
  PositionResponse response;
  response.set_position(position);
  send(from(), response);
}


void ReplicatedLogProcess::truncationRequest(uint64_t position)
{
  LOG(FATAL) << "unimplemented";
}


void ReplicatedLogProcess::connected()
{
  LOG(INFO) << "Log connected to ZooKeeper";

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

    LOG(INFO) << "Log trying to create znode '" << prefix << "' in ZooKeeper";

    // Create the node (even if it already exists).
    ret = zk->create(prefix, "", ZOO_OPEN_ACL_UNSAFE,
		     // ZOO_CREATOR_ALL_ACL, // needs authentication
		     0, &result);

    if (ret != ZOK && ret != ZNODEEXISTS) {
      LOG(FATAL) << "Failed to create '" << prefix
                 << "' in ZooKeeper: " << zk->error(ret);
    }
  }

  // Set a watch on the children of the directory.
  ret = zk->getChildren(znode, true, NULL);

  if (ret != ZOK) {
    LOG(FATAL) << "Failed to set a watch on '" << znode
               << "' in ZooKeeper: " << zk->error(ret);
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
  if ((index = result.find_last_of('/')) != string::npos) {
    result = result.erase(0, index + 1);
  }

  try {
    sequence = boost::lexical_cast<uint64_t>(result);
  } catch (boost::bad_lexical_cast&) {
    LOG(FATAL) << "Failed to convert '" << result << "' into an integer";
  }
}


void ReplicatedLogProcess::reconnecting()
{
  LOG(INFO) << "Log reconnecting to ZooKeeper";
}


void ReplicatedLogProcess::reconnected()
{
  LOG(INFO) << "Log reconnected to ZooKeeper";
}


void ReplicatedLogProcess::expired()
{
  LOG(FATAL) << "Log ZooKeeper session expired!";
}


void ReplicatedLogProcess::updated(const string& path)
{
  LOG(INFO) << "Log notified of updates at '" << path << "' in ZooKeeper";

  CHECK(path == znode);

  // Determine which replica is the master/coordinator.
  vector<string> results;

  int ret = zk->getChildren(znode, true, &results);

  if (ret != ZOK) {
    LOG(FATAL) << "Failed to get children of '" << znode
               << "' in ZooKeeper: " << zk->error(ret);
  }

  // Determine the "minimum" ephemeral znode, that replica acts as the
  // master/coordinator.
  uint64_t min = LONG_MAX;
  foreach (const string& result, results) {
    try {
      min = std::min(min, boost::lexical_cast<uint64_t>(result));
    } catch (boost::bad_lexical_cast&) {
      LOG(FATAL) << "Failed to convert '" << result << "' into an integer";
    }
  }

  LOG(INFO) << "Log elected replica " << min << " master/coordinator";

  // Check our master status.
  if (master && min != sequence) {
    // TODO(benh): Handle this differently?
    LOG(FATAL) << "Log no longer master/coordinator!";
  } else if (!master && min == sequence) {
    LOG(INFO) << "Log elected master/coordinator!";

    master = true;

    // Save all the pids of the replicas (except ourself).
    pids.clear();

    foreach (const string& result, results) {
      string temp;
      ret = zk->get(znode + "/" + result, false, &temp, NULL);

      if (ret == ZNONODE) {
        // Replica must have since been lost.
        continue;
      } else if (ret != ZOK) {
        LOG(FATAL) << "Failed to get data at '" << znode << "/" << result
                   << "' in ZooKeeper: " << zk->error(ret);
      }

      if (self() != temp) {
        pids.insert(UPID(temp));
      }
    }

    // TODO(benh): Need to wait until there are enough replicas
    // available to make a quorum ... code needs to be refactored to
    // support this.

    // Get a "consensus" (quorum - 1) on the last appended position.
    Result<uint64_t> result = Result<uint64_t>::none();

    do {
      // TODO(benh): Eventually fail?
      result = getPositionConsensus(pids, replicas / 2, 1.0);
    } while (result.isNone());

    // TODO(benh): Make sure that our local log is up to date.
    CHECK(position == result.get());

    LOG(INFO) << "Log resuming at position " << position;
  }
}

}} // namespace mesos { namespace internal {


using namespace mesos;
using namespace mesos::internal;


int main(int argc, char** argv)
{
  if (argc != 5) {
    fatal("usage: %s <file> <servers> <znode> <replicas>", argv[0]);
  }

  string file = argv[1];
  string servers = argv[2];
  string znode = argv[3];
  int replicas = atoi(argv[4]);

  ReplicatedLogProcess replica(file, servers, znode, replicas);

  process::wait(process::spawn(&replica));

  return 0;
}

