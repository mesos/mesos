#ifndef __PROCESS_BASED_ISOLATION_MODULE_HPP__
#define __PROCESS_BASED_ISOLATION_MODULE_HPP__

#include <sys/types.h>

#include <boost/unordered_map.hpp>

#include "isolation_module.hpp"
#include "slave.hpp"

#include "launcher/launcher.hpp"

#include "messaging/messages.hpp"


namespace mesos { namespace internal { namespace slave {

class ProcessBasedIsolationModule : public IsolationModule {
public:
  ProcessBasedIsolationModule();

  virtual ~ProcessBasedIsolationModule();

  virtual void initialize(const process::PID<Slave>& slave,
                          const Configuration& conf,
                          bool local);

  virtual pid_t launchExecutor(const FrameworkID& frameworkId,
                               const FrameworkInfo& frameworkInfo,
                               const ExecutorInfo& executorInfo,
                               const std::string& directory);

  virtual void killExecutor(const FrameworkID& frameworkId,
                            const FrameworkInfo& frameworkInfo,
                            const ExecutorInfo& executorInfo);

  virtual void resourcesChanged(const FrameworkID& frameworkId,
                                const FrameworkInfo& frameworkInfo,
                                const ExecutorInfo& executorInfo,
                                const Resources& resources);

protected:
  // Main method executed after a fork() to create a Launcher for launching
  // an executor's process. The Launcher will create the child's working
  // directory, chdir() to it, fetch the executor, set environment varibles,
  // switch user, etc, and finally exec() the executor process.
  // Subclasses of ProcessBasedIsolationModule that wish to override the
  // default launching behavior should override createLauncher() and return
  // their own Launcher object (including possibly a subclass of Launcher).
  virtual launcher::ExecutorLauncher* createExecutorLauncher(const FrameworkID& frameworkId,
                                                             const FrameworkInfo& frameworkInfo,
                                                             const ExecutorInfo& executorInfo,
                                                             const std::string& directory);

private:
  process::PID<Slave> slave;
  // TODO(benh): Make this const by passing it via constructor.
  Configuration conf;
  bool local;
  bool initialized;
  boost::unordered_map<FrameworkID, boost::unordered_map<ExecutorID, pid_t> > pgids;
};

}}}

#endif /* __PROCESS_BASED_ISOLATION_MODULE_HPP__ */
