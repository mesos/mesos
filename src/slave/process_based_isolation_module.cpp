#include <map>

#include "process_based_isolation_module.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::slave;

using boost::unordered_map;

using launcher::ExecutorLauncher;

using process::PID;

using std::map;
using std::string;


ProcessBasedIsolationModule::ProcessBasedIsolationModule()
  : initialized(false) {}


ProcessBasedIsolationModule::~ProcessBasedIsolationModule() {}


void ProcessBasedIsolationModule::initialize(const PID<Slave>& slave,
                                             const Configuration& conf,
                                             bool local)
{
  this->slave = slave;
  this->conf = conf;
  this->local = local;
  this->initialized = true;
}


pid_t ProcessBasedIsolationModule::launchExecutor(const FrameworkID& frameworkId,
                                                  const FrameworkInfo& frameworkInfo,
                                                  const ExecutorInfo& executorInfo,
                                                  const string& directory)
{
  if (!initialized) {
    LOG(FATAL) << "Cannot launch executors before initialization!";
  }

  LOG(INFO) << "Starting executor for framework " << frameworkId
            << ": " << executorInfo.uri();

  pid_t pid;
  if ((pid = fork()) == -1) {
    PLOG(ERROR) << "Failed to fork to launch new executor";
  }

  if (pid) {
    // In parent process, record the pgid for killpg later.
    LOG(INFO) << "Started executor, OS pid = " << pid;
    pgids[frameworkId][executorInfo.executor_id()] = pid;
  } else {
    // In child process, make cleanup easier.
//     if (setpgid(0, 0) < 0)
//       PLOG(FATAL) << "Failed to put executor in own process group";
    if ((pid = setsid()) == -1) {
      PLOG(FATAL) << "Failed to put executor in own session";
    }
    
    createExecutorLauncher(frameworkId, frameworkInfo,
                           executorInfo, directory)->run();
  }

  return pid;
}


void ProcessBasedIsolationModule::killExecutor(const FrameworkID& frameworkId,
                                               const FrameworkInfo& frameworkInfo,
                                               const ExecutorInfo& executorInfo)
{
  if (pgids[frameworkId][executorInfo.executor_id()] != -1) {
    // TODO(benh): Consider sending a SIGTERM, then after so much time
    // if it still hasn't exited do a SIGKILL (can use a libprocess
    // process for this).
    LOG(INFO) << "Sending SIGKILL to gpid "
              << pgids[frameworkId][executorInfo.executor_id()];
    killpg(pgids[frameworkId][executorInfo.executor_id()], SIGKILL);
    pgids[frameworkId][executorInfo.executor_id()] = -1;

    // TODO(benh): Kill all of the process's descendants? Perhaps
    // create a new libprocess process that continually tries to kill
    // all the processes that are a descendant of the executor, trying
    // to kill the executor last ... maybe this is just too much of a
    // burden?

    pgids[frameworkId].erase(executorInfo.executor_id());
  }
}


void ProcessBasedIsolationModule::resourcesChanged(const FrameworkID& frameworkId,
                                                   const FrameworkInfo& frameworkInfo,
                                                   const ExecutorInfo& executorInfo,
                                                   const Resources& resources)
{
  // Do nothing; subclasses may override this.
}


ExecutorLauncher* ProcessBasedIsolationModule::createExecutorLauncher(const FrameworkID& frameworkId,
                                                                      const FrameworkInfo& frameworkInfo,
                                                                      const ExecutorInfo& executorInfo,
                                                                      const string& directory)
{
  // Create a map of parameters for the executor launcher.
  map<string, string> params;

  for (int i = 0; i < executorInfo.params().param_size(); i++) {
    params[executorInfo.params().param(i).key()] = 
      executorInfo.params().param(i).value();
  }

  return new ExecutorLauncher(frameworkId,
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
}
