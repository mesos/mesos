#ifndef __LXC_ISOLATION_MODULE_HPP__
#define __LXC_ISOLATION_MODULE_HPP__

#include <string>

#include "isolation_module.hpp"
#include "reaper.hpp"
#include "slave.hpp"

#include "common/hashmap.hpp"


namespace mesos { namespace internal { namespace slave {

class LxcIsolationModule
  : public IsolationModule, public ProcessExitedListener
{
public:
  LxcIsolationModule();

  virtual ~LxcIsolationModule();

  virtual void initialize(const Configuration& conf,
                          bool local,
                          const process::PID<Slave>& slave);

  virtual void launchExecutor(const FrameworkID& frameworkId,
                              const FrameworkInfo& frameworkInfo,
                              const ExecutorInfo& executorInfo,
                              const std::string& directory);

  virtual void killExecutor(const FrameworkID& frameworkId,
                            const ExecutorID& executorId);

  virtual void resourcesChanged(const FrameworkID& frameworkId,
                                const ExecutorID& executorId,
                                const Resources& resources);

  virtual void processExited(pid_t pid, int status);

protected:
  // Run a shell command formatted with varargs and return its exit code.
  int shell(const char* format, ...);

  // Attempt to set a resource limit of a container for a given cgroup
  // property (e.g. cpu.shares). Returns true on success.
  bool setResourceLimit(const std::string& container,
			const std::string& property,
                        int64_t value);

private:
  // No copying, no assigning.
  LxcIsolationModule(const LxcIsolationModule&);
  LxcBasedIsolationModule& operator = (const LxcIsolationModule&);

  // Per-framework information object maintained in info hashmap.
  struct FrameworkInfo
  {
    std::string container; // Name of Linux container used for this framework.
    pid_t pid; // PID of lxc-execute command running the executor.
  };

  // TODO(benh): Make variables const by passing them via constructor.
  Configuration conf;
  bool local;
  process::PID<Slave> slave;
  bool initialized;
  Reaper* reaper;
  hashmap<FrameworkID, hashmap<ExecutorID, FrameworkInfo*> > infos;
};

}}} // namespace mesos { namespace internal { namespace slave {

#endif // __LXC_ISOLATION_MODULE_HPP__
