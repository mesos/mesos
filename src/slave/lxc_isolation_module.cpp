#include <map>

#include <algorithm>
#include <stdlib.h>
#include <unistd.h>

#include <process/dispatch.hpp>

#include "lxc_isolation_module.hpp"

#include "common/foreach.hpp"

#include "launcher/launcher.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::slave;

using namespace process;

using launcher::ExecutorLauncher;

using std::map;
using std::string;

namespace {

const int32_t CPU_SHARES_PER_CPU = 1024;
const int32_t MIN_CPU_SHARES = 10;
const int64_t MIN_RSS = 128 * Megabyte;

}


LxcIsolationModule::LxcIsolationModule()
  : initialized(false)
{
  // Spawn the reaper, note that it might send us a message before we
  // actually get spawned ourselves, but that's okay, the message will
  // just get dropped.
  reaper = new Reaper();
  spawn(reaper);
  dispatch(reaper, &Reaper::addProcessExitedListener, this);
}


LxcIsolationModule::~LxcIsolationModule()
{
  CHECK(reaper != NULL);
  terminate(reaper);
  wait(reaper);
  delete reaper;
}


void LxcIsolationModule::initialize(
    const Configuration& _conf,
    bool _local,
    const PID<Slave>& _slave)
{
  conf = _conf;
  local = _local;
  slave = _slave;
  
  // Check if Linux Container tools are available.
  if (system("lxc-version > /dev/null") != 0) {
    LOG(FATAL) << "Could not run lxc-version; make sure Linux Container "
                << "tools are installed";
  }

  // Check that we are root (it might also be possible to create Linux
  // containers without being root, but we can support that later).
  if (getuid() != 0) {
    LOG(FATAL) << "LXC isolation module requires slave to run as root";
  }

  initialized = true;
}


void LxcIsolationModule::launchExecutor(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const string& directory)
{
  if (!initialized) {
    LOG(FATAL) << "Cannot launch executors before initialization!";
  }

  LOG(INFO) << "Launching '" << executorInfo.uri()
            << "' for executor '" << executorInfo.executor_id()
            << "' of framework " << frameworkId;

  infos[frameworkId][executorInfo.executor_id()] = new FrameworkInfo();

  // Get location of Mesos install in order to find mesos-launcher.
  string mesosLauncher = conf.get("home", ".") + "/mesos-launcher";

  // Create a name for the container.
  ostringstream out;
  out << "mesos.executor-" << executorInfo.executor_id()
      << ".framework-" << frameworkId;

  string containerName = out.str();

  infos[frameworkId][executorInfo.executor_id()]->container = containerName;

  // Run lxc-execute mesos-launcher using a fork-exec (since lxc-execute
  // does not return until the container is finished). Note that lxc-execute
  // automatically creates the container and will delete it when finished.
  pid_t pid;
  if ((pid = fork()) == -1) {
    PLOG(FATAL) << "Failed to fork to launch lxc-execute";
  }

  if (pid) {
    // In parent process.
    infos[frameworkId][executorInfo.executor_id()]->pid = pid;

    // Tell the slave this executor has started.
    dispatch(slave, &Slave::executorStarted,
             frameworkId, executorInfo.executor_id(), pid);
  } else {
    // Create an ExecutorLauncher to set up the environment for executing
    // an external launcher_main.cpp process (inside of lxc-execute).
    map<string, string> params;

    for (int i = 0; i < executorInfo.params().param_size(); i++) {
      params[executorInfo.params().param(i).key()] =
        executorInfo.params().param(i).value();
    }

    ExecutorLauncher* launcher =
      new ExecutorLauncher(frameworkId,
			   executorInfo.executor_id(),
			   executorInfo.uri(),
			   frameworkInfo.user(),
                           directory,
			   slave,
			   conf.get("frameworks_home", ""),
			   conf.get("home", ""),
			   conf.get("hadoop_home", ""),
			   !local,
			   conf.get("switch_user", true),
			   params);

    launcher->setupEnvironmentForLauncherMain();
    
    // Run lxc-execute.
    execlp("lxc-execute", "lxc-execute", "-n", containerName.c_str(),
           mesosLauncher.c_str(), (char *) NULL);

    // If we get here, the execlp call failed.
    LOG(FATAL) << "Could not exec lxc-execute";
  }
}


void LxcIsolationModule::killExecutor(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  if (!infos.countains(frameworkId) ||
      !infos[frameworkId].contains(executorId)) {
    LOG(ERROR) << "ERROR! Asked to kill an unknown executor!";
    return;
  }

  FrameworkInfo* info = infos[frameworkId][executorId];

  if (info->container != "") {
    LOG(INFO) << "Stopping container " << info->container;

    int ret = shell("lxc-stop -n %s", info->container.c_str());
    if (ret != 0) {
      LOG(ERROR) << "lxc-stop returned " << ret;
    }

    infos[frameworkId].erase(executorId);
    delete info;

    if (infos[frameworkId].size() == 0) {
      infos.erase(frameworkId);
    }
  }
}


void LxcIsolationModule::resourcesChanged(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Resources& resources)
{
  if (!infos.contains(frameworkId) ||
      !infos[frameworkId].contains(executorId)) {
    LOG(ERROR) << "ERROR! Asked to update resources for an unknown executor!";
    return;
  }

  if (infos[frameworkId][executorId]->container != "") {
    // For now, just try setting the CPUs and memory right away, and kill the
    // framework if this fails.
    // A smarter thing to do might be to only update them periodically in a
    // separate thread, and to give frameworks some time to scale down their
    // memory usage.

    double cpu = resources.getScalar("cpu", Resource::Scalar()).value();
    int32_t cpuShares = max(CPU_SHARES_PER_CPU * (int32_t) cpu, MIN_CPU_SHARES);
    if (!setResourceLimit(frameworkId, executorId, "cpu.shares", cpuShares)) {
      // TODO(benh): Kill the executor, but do it in such a way that
      // the slave finds out about it exiting.
      return;
    }

    double mem = resources.getScalar("mem", Resource::Scalar()).value();
    int64_t rssLimit = max((int64_t) mem, MIN_RSS) * 1024LL * 1024LL;
    if (!setResourceLimit(frameworkId, executorId, "memory.limit_in_bytes", rssLimit)) {
      // TODO(benh): Kill the executor, but do it in such a way that
      // the slave finds out about it exiting.
      return;
    }
  }
}


bool LxcIsolationModule::setResourceLimit(
    const string& container,
    const string& property,
    int64_t value)
{
  LOG(INFO) << "Setting " << property
            << " for container " << << container
            << " to " << value;

  int ret = shell("lxc-cgroup -n %s %s %lld",
                  container.c_str(),
                  property.c_str(),
                  value);
  if (ret != 0) {
    LOG(ERROR) << "Failed to set " << property
               << " for container " << framework->frameworkId
               << ": lxc-cgroup returned " << ret;
    return false;
  } else {
    return true;
  }
}


// TODO(benh): Factor this out into common/utils or possibly into
// libprocess so that it can handle blocking.
int LxcIsolationModule::shell(const char* fmt, ...)
{
  char* cmd;
  FILE* f;
  int ret;
  va_list args;
  va_start(args, fmt);
  if (vasprintf(&cmd, fmt, args) == -1)
    return -1;
  if ((f = popen(cmd, "w")) == NULL)
    return -1;
  ret = pclose(f);
  if (ret == -1)
    LOG(INFO) << "pclose error: " << strerror(errno);
  free(cmd);
  va_end(args);
  return ret;
}


void LxcIsolationModule::processExited(pid_t pid, int status)
{
  foreachkey (const FrameworkID& frameworkId, infos) {
    foreachpair (const ExecutorID& executorId, FrameworkInfo* info, infos[frameworkId]) {
      if (info->pid == pid) {
        // Kill the container.
        killExecutor(frameworkId, executorId);

        LOG(INFO) << "Telling slave of lost executor " << executorId
                  << " of framework " << frameworkId;

        dispatch(slave, &Slave::executorExited,
                 frameworkId, executorId, status);
        break;
      }
    }
  }
}
