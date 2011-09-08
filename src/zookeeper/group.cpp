#include <algorithm>
#include <map>
#include <queue>
#include <utility>
#include <vector>

#include <process/process.hpp>
#include <process/timer.hpp>

#include "common/result.hpp"
#include "common/strings.hpp"
#include "common/utils.hpp"

#include "zookeeper/group.hpp"
#include "zookeeper/watcher.hpp"
#include "zookeeper/zookeeper.hpp"

using namespace process;

namespace utils = mesos::internal::utils; // TODO(benh): Pull utils out.

using process::wait; // Necessary on some OS's to disambiguate.

using std::make_pair;
using std::map;
using std::queue;
using std::set;
using std::string;
using std::vector;


namespace zookeeper {

const double RETRY_SECONDS = 2.0; // Time to wait after retryable errors.

class GroupProcess : public Process<GroupProcess>
{
public:
  GroupProcess(const string& servers,
               const seconds& timeout,
               const string& znode);
  ~GroupProcess();

  void initialize();

  // Group implementation.
  Promise<Group::Membership> join(const string& info);
  Promise<bool> cancel(const Group::Membership& membership);
  Promise<string> info(const Group::Membership& membership);
  Promise<set<Group::Membership> > watch(
      const set<Group::Membership>& expected);
  Promise<Option<int64_t> > session();

  // ZooKeeper events.
  void connected(bool reconnect);
  void reconnecting();
  void expired();
  void updated(const string& path);
  void created(const string& path);
  void deleted(const string& path);

private:
  Result<Group::Membership> doJoin(const std::string& info);
  Result<bool> doCancel(const Group::Membership& membership);
  Result<string> doInfo(const Group::Membership& membership);

  // Synchronizes pending operations with ZooKeeper (i.e., performs
  // joins, cancels, infos, etc).
  bool sync();

  // Generic retry method. This mechanism is "generic" in the sense
  // that it is not specific to any particular operation, but rather
  // attempts to perform all pending operations.
  void retry(double seconds);

  Option<std::string> error; // Potential non-retryable error.

  const std::string servers;
  const seconds timeout;
  const std::string znode;
  const ACL_vector acl;

  Watcher* watcher;
  ZooKeeper* zk;

  enum State { // ZooKeeper connection state.
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
  } state;

  struct Join
  {
    Join(const string& _info) : info(_info) {}
    string info;
    Promise<Group::Membership> promise;
  };

  struct Cancel
  {
    Cancel(const Group::Membership& _membership)
      : membership(_membership) {}
    Group::Membership membership;
    Promise<bool> promise;
  };

  struct Info
  {
    Info(const Group::Membership& _membership)
      : membership(_membership) {}
    Group::Membership membership;
    Promise<string> promise;
  };

  struct Watch
  {
    Promise<set<Group::Membership> > promise;
  };

  struct {
    queue<Join> joins;
    queue<Cancel> cancels;
    queue<Info> infos;
    queue<Watch> watches;
  } pending;

  bool retrying;

  map<Group::Membership, string> owned;

  set<Group::Membership> memberships; // Cache of "all" memberships.
};


GroupProcess::GroupProcess(
    const string& _servers,
    const seconds& _timeout,
    const string& _znode)
  : servers(_servers),
    timeout(_timeout),
    znode(strings::remove(_znode, "/", strings::SUFFIX)),
    acl(ZOO_OPEN_ACL_UNSAFE),
    state(DISCONNECTED),
    retrying(false)
{}


GroupProcess::~GroupProcess()
{
  delete zk;
  delete watcher;
}


void GroupProcess::initialize()
{
  // Doing initialization here allows to avoid the race between
  // instantiating the ZooKeeper instance and being spawned ourself.
  watcher = new ProcessWatcher<GroupProcess>(self());
  zk = new ZooKeeper(servers, timeout, watcher);
  state = CONNECTING;
}


Promise<Group::Membership> GroupProcess::join(const string& info)
{
  if (error.isSome()) {
    Promise<Group::Membership> promise;
    promise.fail(error.get());
    return promise;
  } else if (state != CONNECTED) {
    Join join(info);
    pending.joins.push(join);
    return join.promise;
  }

  // TODO(benh): Write a test to see how ZooKeeper fails setting znode
  // data when the data is larger than 1 MB so we know whether or not
  // to check for that here.

  Result<Group::Membership> membership = doJoin(info);

  if (membership.isNone()) { // Try again later.
    if (!retrying) {
      delay(RETRY_SECONDS, self(), &GroupProcess::retry, RETRY_SECONDS);
      retrying = true;
    }
    Join join(info);
    pending.joins.push(join);
    return join.promise;
  } else if (membership.isError()) {
    Promise<Group::Membership> promise;
    promise.fail(membership.error());
    return promise;
  }

  owned.insert(make_pair(membership.get(), info));

  return membership.get();
}


Promise<bool> GroupProcess::cancel(const Group::Membership& membership)
{
  if (error.isSome()) {
    Promise<bool> promise;
    promise.fail(error.get());
    return promise;
  } else if (owned.count(membership) == 0) {
    return false; // TODO(benh): Should this be an error?
  }

  if (state != CONNECTED) {
    Cancel cancel(membership);
    pending.cancels.push(cancel);
    return cancel.promise;
  }

  Result<bool> cancellation = doCancel(membership);

  if (cancellation.isNone()) { // Try again later.
    if (!retrying) {
      delay(RETRY_SECONDS, self(), &GroupProcess::retry, RETRY_SECONDS);
      retrying = true;
    }
    Cancel cancel(membership);
    pending.cancels.push(cancel);
    return cancel.promise;
  } else if (cancellation.isError()) {
    Promise<bool> promise;
    promise.fail(cancellation.error());
    return promise;
  }

  return cancellation.get();
}


Promise<string> GroupProcess::info(const Group::Membership& membership)
{
  if (error.isSome()) {
    Promise<string> promise;
    promise.fail(error.get());
    return promise;
  } else if (state != CONNECTED) {
    Info info(membership);
    pending.infos.push(info);
    return info.promise;
  }

  Result<string> result = doInfo(membership);

  if (result.isNone()) { // Try again later.
    Info info(membership);
    pending.infos.push(info);
    return info.promise;
  } else if (result.isError()) {
    Promise<string> promise;
    promise.fail(result.error());
    return promise;
  }

  return result.get();
}


Promise<set<Group::Membership> > GroupProcess::watch(
    const set<Group::Membership>& expected)
{
  if (error.isSome()) {
    Promise<set<Group::Membership> > promise;
    promise.fail(error.get());
    return promise;
  } else if (memberships != expected) {
    return memberships;
  }

  Watch watch;
  pending.watches.push(watch);
  return watch.promise;
}


Promise<Option<int64_t> > GroupProcess::session()
{
  if (error.isSome()) {
    Promise<Option<int64_t> > promise;
    promise.fail(error.get());
    return promise;
  } else if (state != CONNECTED) {
    return Option<int64_t>::none();
  }

  return Option<int64_t>::some(zk->getSessionId());
}


void GroupProcess::connected(bool reconnect)
{
  if (!reconnect) {
    CHECK(znode.size() == 0 || znode.at(znode.size() - 1) != '/');

    // Create directory path znodes as necessary.
    size_t index = znode.find("/", 0);

    while (index < string::npos) {
      // Get out the prefix to create.
      index = znode.find("/", index + 1);
      const string& prefix = znode.substr(0, index);

      LOG(INFO) << "Trying to create '" << prefix << "' in ZooKeeper";

      // Create the node (even if it already exists).
      int code = zk->create(prefix, "", acl, 0, NULL);

      if (code == ZINVALIDSTATE || (code != ZOK && zk->retryable(code))) {
        // TODO(benh): Handle authentication..
        CHECK(zk->getState() != ZOO_AUTH_FAILED_STATE);
        return; // Try again later.
      } else if (code != ZOK && code != ZNODEEXISTS) {
        Try<string> message = strings::format(
            "Failed to create '%s' in ZooKeeper: %s",
            prefix.c_str(), zk->message(code));
        error = message.isSome()
          ? message.get()
          : "Failed to create node in ZooKeeper";
        return; // TODO(benh): Everything pending is still pending!
      }
    }
  }

  state = CONNECTED;

  LOG(INFO) << (reconnect ? "Re-c" : "C") << "onnected to ZooKeeper";

  sync(); // Handle pending.

  updated(znode); // Also sets a watch on the children.
}


void GroupProcess::reconnecting()
{
  state = CONNECTING;
}


void GroupProcess::expired()
{
  // No need to clear memberships, next time we are connected we'll
  // re-run GroupProcess::updated and handle any changes.
  owned.clear();
  state = DISCONNECTED;
  delete zk;
  zk = new ZooKeeper(servers, timeout, watcher);
  state = CONNECTING;
}


void GroupProcess::updated(const string& path)
{
  CHECK(znode == path);

  // Check for any new memberships.
  vector<string> results;

  int code = zk->getChildren(znode, true, &results); // Sets the watch!

  if (code == ZINVALIDSTATE || (code != ZOK && zk->retryable(code))) {
    // TODO(benh): Handle authentication..
    CHECK(zk->getState() != ZOO_AUTH_FAILED_STATE);
    return;
  } else if (code != ZOK) {
    Try<string> message = strings::format(
        "Non-retryable error attempting to get children of '%s'"
        " in ZooKeeper: %s", znode.c_str(), zk->message(code));
    error = message.isSome()
      ? message.get()
      : "Non-retryable error attempting to get children in ZooKeeper";
    return; // TODO(benh): Everything pending is still pending!
  }

  // Collect all the current memberships.
  set<Group::Membership> current;

  foreach (const string& result, results) {
    Try<uint64_t> sequence = utils::numify<uint64_t>(result);

    // Skip it if it couldn't be converted to a number.
    if (sequence.isError()) {
      LOG(WARNING) << "Found non-sequence node '" << result
                   << "' at '" << znode << "' in ZooKeeper";
      continue;
    }

    current.insert(Group::Membership(sequence.get()));
  }

  if (memberships != current) {
    // Invoke the watches.
    while (!pending.watches.empty()) {
      pending.watches.front().promise.set(current);
      pending.watches.pop();
    }

    memberships = current;
  }
}


void GroupProcess::created(const string& path)
{
  LOG(FATAL) << "Unexpected ZooKeeper event";
}


void GroupProcess::deleted(const string& path)
{
  LOG(FATAL) << "Unexpected ZooKeeper event";
}


Result<Group::Membership> GroupProcess::doJoin(const string& info)
{
  CHECK(error.isNone());
  CHECK(state == CONNECTED);

  // Create a new ephemeral node to represent a new member and use the
  // the specified info as it's contents.
  string result;

  int code = zk->create(znode + "/", info, acl,
                        ZOO_SEQUENCE | ZOO_EPHEMERAL, &result);

  if (code == ZINVALIDSTATE || (code != ZOK && zk->retryable(code))) {
    // TODO(benh): Handle authentication..
    CHECK(zk->getState() != ZOO_AUTH_FAILED_STATE);
    return Result<Group::Membership>::none();
  } else if (code != ZOK) {
    Try<string> message = strings::format(
        "Failed to create ephemeral node at '%s' in ZooKeeper: %s",
        znode.c_str(), zk->message(code));
    return Result<Group::Membership>::error(
        message.isSome() ? message.get()
        : "Failed to create ephemeral node in ZooKeeper");
  }

  // Save the sequence number but only grab the basename. Example:
  // "/path/to/znode/0000000131" => "0000000131".
  result = utils::os::basename(result);

  Try<uint64_t> sequence = utils::numify<uint64_t>(result);
  CHECK(sequence.isSome()) << sequence.error();

  Group::Membership membership(sequence.get());

  return membership;
}


Result<bool> GroupProcess::doCancel(const Group::Membership& membership)
{
  CHECK(error.isNone());
  CHECK(state == CONNECTED);

  Try<string> sequence = strings::format("%.*d", 10, membership.sequence);

  CHECK(sequence.isSome()) << sequence.error();

  string path = znode + "/" + sequence.get();

  LOG(INFO) << "Trying to remove '" << path << "' in ZooKeeper";

  // Remove ephemeral node.
  int code = zk->remove(path, -1);

  if (code == ZINVALIDSTATE || (code != ZOK && zk->retryable(code))) {
    // TODO(benh): Handle authentication..
    CHECK(zk->getState() != ZOO_AUTH_FAILED_STATE);
    return Result<bool>::none();
  } else if (code != ZOK) {
    Try<string> message = strings::format(
        "Failed to remove ephemeral node '%s' in ZooKeeper: %s",
        path.c_str(), zk->message(code));
    return Result<bool>::error(
        message.isSome() ? message.get()
        : "Failed to remove ephemeral node in ZooKeeper");
  }

  owned.erase(membership);

  return true;
}


Result<string> GroupProcess::doInfo(const Group::Membership& membership)
{
  CHECK(error.isNone());
  CHECK(state == CONNECTED);

  Try<string> sequence = strings::format("%.*d", 10, membership.sequence);

  CHECK(sequence.isSome()) << sequence.error();

  string path = znode + "/" + sequence.get();

  LOG(INFO) << "Trying to get '" << path << "' in ZooKeeper";

  // Get data associated with ephemeral node.
  string result;

  int code = zk->get(path, false, &result, NULL);

  if (code == ZINVALIDSTATE || (code != ZOK && zk->retryable(code))) {
    // TODO(benh): Handle authentication.
    CHECK(zk->getState() != ZOO_AUTH_FAILED_STATE);
    return Result<string>::none();
  } else if (code != ZOK) {
    Try<string> message = strings::format(
        "Failed to get data for ephemeral node '%s' in ZooKeeper: %s",
        path.c_str(), zk->message(code));
    return Result<string>::error(
        message.isSome() ? message.get()
        : "Failed to get data for ephemeral node in ZooKeeper");
  }

  return result;
}


bool GroupProcess::sync()
{
  CHECK(error.isNone());
  CHECK(state == CONNECTED);

  // Do joins.
  while (!pending.joins.empty()) {
    Result<Group::Membership> membership = doJoin(pending.joins.front().info);
    if (membership.isNone()) {
      return false; // Try again later.
    } else if (membership.isError()) {
      pending.joins.front().promise.fail(membership.error());
    } else {
      owned.insert(make_pair(membership.get(), pending.joins.front().info));
      pending.joins.front().promise.set(membership.get());
    }
    pending.joins.pop();
  }

  // Do cancels.
  while (!pending.cancels.empty()) {
    Result<bool> cancellation = doCancel(pending.cancels.front().membership);
    if (cancellation.isNone()) {
      return false; // Try again later.
    } else if (cancellation.isError()) {
      pending.cancels.front().promise.fail(cancellation.error());
    } else {
      pending.cancels.front().promise.set(cancellation.get());
    }
    pending.cancels.pop();
  }

  // Do infos.
  while (!pending.infos.empty()) {
    // TODO(benh): Ignore if future has been discarded?
    Result<string> result = doInfo(pending.infos.front().membership);
    if (result.isNone()) {
      return false; // Try again later.
    } else if (result.isError()) {
      pending.infos.front().promise.fail(result.error());
    } else {
      pending.infos.front().promise.set(result.get());
    }
    pending.infos.pop();
  }

  return true;
}


void GroupProcess::retry(double seconds)
{
  if (error.isSome() || state != CONNECTED) {
    retrying = false; // Stop retrying, we'll sync at reconnect (if no error).
  } else if (error.isNone() && state == CONNECTED) {
    bool synced = sync(); // Might get another retryable error.
    if (!synced) {
      seconds = std::min(seconds * 2.0, 60.0); // Backoff.
      delay(seconds, self(), &GroupProcess::retry, seconds);
    } else {
      retrying = false;
    }
  }
}


Group::Group(const string& servers,
             const seconds& timeout,
             const string& znode)
{
  process = new GroupProcess(servers, timeout, znode);
  spawn(process);
  dispatch(process, &GroupProcess::initialize);
}


Group::~Group()
{
  terminate(process);
  wait(process);
  delete process;
}


Future<Group::Membership> Group::join(const std::string& info)
{
  return dispatch(process, &GroupProcess::join, info);
}


Future<bool> Group::cancel(const Group::Membership& membership)
{
  return dispatch(process, &GroupProcess::cancel, membership);
}


Future<string> Group::info(const Group::Membership& membership)
{
  return dispatch(process, &GroupProcess::info, membership);
}


Future<set<Group::Membership> > Group::watch(
    const set<Group::Membership>& expected)
{
  return dispatch(process, &GroupProcess::watch, expected);
}


Future<Option<int64_t> > Group::session()
{
  return dispatch(process, &GroupProcess::session);
}

} // namespace zookeeper {
