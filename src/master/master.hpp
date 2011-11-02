/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __MASTER_HPP__
#define __MASTER_HPP__

#include <string>
#include <vector>

#include <process/process.hpp>
#include <process/protobuf.hpp>

#include "common/foreach.hpp"
#include "common/hashmap.hpp"
#include "common/hashset.hpp"
#include "common/multihashmap.hpp"
#include "common/resources.hpp"
#include "common/type_utils.hpp"
#include "common/units.hpp"
#include "common/utils.hpp"

#include "configurator/configurator.hpp"

#include "master/constants.hpp"
#include "master/http.hpp"

#include "messages/messages.hpp"


namespace mesos {
namespace internal {
namespace master {

using namespace process; // Included to make code easier to read.

// Some forward declarations.
class Allocator;
class SlavesManager;
struct Framework;
struct Slave;
class SlaveObserver;


class Master : public ProtobufProcess<Master>
{
public:
  Master(Allocator* _allocator);
  Master(Allocator* _allocator, const Configuration& conf);

  virtual ~Master();

  static void registerOptions(Configurator* configurator);

  void submitScheduler(const std::string& name);
  void newMasterDetected(const UPID& pid);
  void noMasterDetected();
  void masterDetectionFailure();
  void registerFramework(const FrameworkInfo& frameworkInfo);
  void reregisterFramework(const FrameworkID& frameworkId,
                           const FrameworkInfo& frameworkInfo,
                           bool failover);
  void unregisterFramework(const FrameworkID& frameworkId);
  void deactivateFramework(const FrameworkID& frameworkId);
  void resourceRequest(const FrameworkID& frameworkId,
                       const std::vector<ResourceRequest>& requests);
  void launchTasks(const FrameworkID& frameworkId,
                   const OfferID& offerId,
                   const std::vector<TaskDescription>& tasks,
                   const Filters& filters);
  void reviveOffers(const FrameworkID& frameworkId);
  void killTask(const FrameworkID& frameworkId, const TaskID& taskId);
  void schedulerMessage(const SlaveID& slaveId,
                        const FrameworkID& frameworkId,
                        const ExecutorID& executorId,
                        const std::string& data);
  void registerSlave(const SlaveInfo& slaveInfo);
  void reregisterSlave(const SlaveID& slaveId,
                       const SlaveInfo& slaveInfo,
                       const std::vector<ExecutorInfo>& executorInfos,
                       const std::vector<Task>& tasks);
  void unregisterSlave(const SlaveID& slaveId);
  void statusUpdate(const StatusUpdate& update, const UPID& pid);
  void executorMessage(const SlaveID& slaveId,
                       const FrameworkID& frameworkId,
                       const ExecutorID& executorId,
                       const std::string& data);
  void exitedExecutor(const SlaveID& slaveId,
                      const FrameworkID& frameworkId,
                      const ExecutorID& executorId,
                      int32_t status);
  void activatedSlaveHostnamePort(const std::string& hostname, uint16_t port);
  void deactivatedSlaveHostnamePort(const std::string& hostname, uint16_t port);
  void timerTick();
  void frameworkFailoverTimeout(const FrameworkID& frameworkId,
                                double reregisteredTime);
  void exited();

  // Return connected frameworks that are not in the process of being removed
  std::vector<Framework*> getActiveFrameworks() const;

  // Return connected slaves that are not in the process of being removed
  std::vector<Slave*> getActiveSlaves() const;

  void makeOffers(Framework* framework,
                  const hashmap<Slave*, Resources>& offered);

protected:
  virtual void operator () ();

  void initialize();

  // Process a launch tasks request (for a non-cancelled offer) by
  // launching the desired tasks (if the offer contains a valid set of
  // tasks) and reporting any unused resources to the allocator.
  void processTasks(Offer* offer,
                    Framework* framework,
                    Slave* slave,
                    const std::vector<TaskDescription>& tasks,
                    const Filters& filters);

  // Add a framework.
  void addFramework(Framework* framework);

  // Replace the scheduler for a framework with a new process ID, in
  // the event of a scheduler failover.
  void failoverFramework(Framework* framework, const UPID& newPid);

  // Kill all of a framework's tasks, delete the framework object, and
  // reschedule offers that were assigned to this framework.
  void removeFramework(Framework* framework);

  // Add a slave.
  void addSlave(Slave* slave, bool reregister = false);

  void readdSlave(Slave* slave,
		  const std::vector<ExecutorInfo>& executorInfos,
		  const std::vector<Task>& tasks);

  // Lose all of a slave's tasks and delete the slave object
  void removeSlave(Slave* slave);

  // Launch a task from a task description, and returned the consumed
  // resources for the task and possibly it's executor.
  Resources launchTask(const TaskDescription& task,
                       Framework* framework,
                       Slave* slave);

  // Remove a task.
  void removeTask(Task* task);

  // Remove an offer and optionally rescind the offer as well.
  void removeOffer(Offer* offer, bool rescind = false);

  Framework* getFramework(const FrameworkID& frameworkId);
  Slave* getSlave(const SlaveID& slaveId);
  Offer* getOffer(const OfferID& offerId);

  FrameworkID newFrameworkId();
  OfferID newOfferId();
  SlaveID newSlaveId();

private:

  // TODO(benh): Remove once SimpleAllocator doesn't use Master::get*.
  friend class SimpleAllocator;
  friend struct SlaveRegistrar;
  friend struct SlaveReregistrar;

  // Http handlers, friends of the master in order to access state,
  // they get invoked from within the master so there is no need to
  // use synchronization mechanisms to protect state.
  friend Promise<HttpResponse> http::vars(
      const Master& master,
      const HttpRequest& request);

  friend Promise<HttpResponse> http::json::stats(
      const Master& master,
      const HttpRequest& request);

  friend Promise<HttpResponse> http::json::state(
      const Master& master,
      const HttpRequest& request);

  const Configuration conf;

  bool elected;

  Allocator* allocator;
  SlavesManager* slavesManager;

  // Contains the date the master was launched and some ephemeral
  // token (e.g. returned from ZooKeeper). Used in framework and slave
  // IDs created by this master.
  std::string id;

  multihashmap<std::string, uint16_t> slaveHostnamePorts;

  hashmap<FrameworkID, Framework*> frameworks;
  hashmap<SlaveID, Slave*> slaves;
  hashmap<OfferID, Offer*> offers;

  std::list<Framework*> completedFrameworks;
  int maxCompletedFrameworks;

  int64_t nextFrameworkId; // Used to give each framework a unique ID.
  int64_t nextOfferId;     // Used to give each slot offer a unique ID.
  int64_t nextSlaveId;     // Used to give each slave a unique ID.

  // Statistics (initialized in Master::initialize).
  struct {
    uint64_t tasks[TaskState_ARRAYSIZE];
    uint64_t validStatusUpdates;
    uint64_t invalidStatusUpdates;
    uint64_t validFrameworkMessages;
    uint64_t invalidFrameworkMessages;
  } stats;

  // Start time used to calculate uptime.
  double startTime;

  // Failover timeout for frameworks, in seconds.
  int failoverTimeout;
};


// A connected slave.
struct Slave
{
  Slave(const SlaveInfo& _info,
        const SlaveID& _id,
        const UPID& _pid,
        double time)
    : info(_info),
      id(_id),
      pid(_pid),
      active(true),
      registeredTime(time),
      lastHeartbeat(time) {}

  ~Slave() {}

  Task* getTask(const FrameworkID& frameworkId, const TaskID& taskId)
  {
    foreachvalue (Task* task, tasks) {
      if (task->framework_id() == frameworkId &&
          task->task_id() == taskId) {
        return task;
      }
    }

    return NULL;
  }

  void addTask(Task* task)
  {
    std::pair<FrameworkID, TaskID> key =
      std::make_pair(task->framework_id(), task->task_id());
    CHECK(tasks.count(key) == 0);
    tasks[key] = task;
    resourcesInUse += task->resources();
  }

  void removeTask(Task* task)
  {
    std::pair<FrameworkID, TaskID> key =
      std::make_pair(task->framework_id(), task->task_id());
    CHECK(tasks.count(key) > 0);
    tasks.erase(key);
    resourcesInUse -= task->resources();
  }

  void addOffer(Offer* offer)
  {
    CHECK(!offers.contains(offer));
    offers.insert(offer);
    resourcesOffered += offer->resources();
  }

  void removeOffer(Offer* offer)
  {
    CHECK(offers.contains(offer));
    offers.erase(offer);
    resourcesOffered -= offer->resources();
  }

  bool hasExecutor(const FrameworkID& frameworkId,
		   const ExecutorID& executorId)
  {
    return executors.contains(frameworkId) &&
      executors[frameworkId].contains(executorId);
  }

  void addExecutor(const FrameworkID& frameworkId,
		   const ExecutorInfo& executorInfo)
  {
    CHECK(!hasExecutor(frameworkId, executorInfo.executor_id()));
    executors[frameworkId][executorInfo.executor_id()] = executorInfo;

    // Update the resources in use to reflect running this executor.
    resourcesInUse += executorInfo.resources();
  }

  void removeExecutor(const FrameworkID& frameworkId,
		      const ExecutorID& executorId)
  {
    if (hasExecutor(frameworkId, executorId)) {
      // Update the resources in use to reflect removing this executor.
      resourcesInUse -= executors[frameworkId][executorId].resources();

      executors[frameworkId].erase(executorId);
      if (executors[frameworkId].size() == 0) {
        executors.erase(frameworkId);
      }
    }
  }

  Resources resourcesFree()
  {
    return info.resources() - (resourcesOffered + resourcesInUse);
  }

  const SlaveID id;
  const SlaveInfo info;

  UPID pid;

  bool active; // Turns false when slave is being removed.
  double registeredTime;
  double lastHeartbeat;

  Resources resourcesOffered; // Resources currently in offers.
  Resources resourcesInUse;   // Resources currently used by tasks.

  // Executors running on this slave.
  hashmap<FrameworkID, hashmap<ExecutorID, ExecutorInfo> > executors;

  // Tasks running on this slave, indexed by FrameworkID x TaskID.
  hashmap<std::pair<FrameworkID, TaskID>, Task*> tasks;

  // Active offers on this slave.
  hashset<Offer*> offers;

  SlaveObserver* observer;
};


// An connected framework.
struct Framework
{
  Framework(const FrameworkInfo& _info,
	    const FrameworkID& _id,
            const UPID& _pid,
	    double time)
    : info(_info),
      id(_id),
      pid(_pid),
      active(true),
      registeredTime(time),
      reregisteredTime(time) {
	  maxCompletedTasks = 100;
  }

  ~Framework() {}

  Task* getTask(const TaskID& taskId)
  {
    if (tasks.count(taskId) > 0) {
      return tasks[taskId];
    } else {
      return NULL;
    }
  }

  void addTask(Task* task)
  {
    CHECK(!tasks.contains(task->task_id()));
    tasks[task->task_id()] = task;
    resources += task->resources();
  }

  void removeTask(Task* task)
  {
    CHECK(tasks.contains(task->task_id()));

    completedTasks.push_back(task);

    if(completedTasks.size() > maxCompletedTasks) {
    	completedTasks.pop_front();
    }

    tasks.erase(task->task_id());
    resources -= task->resources();
  }

  void addOffer(Offer* offer)
  {
    CHECK(!offers.contains(offer));
    offers.insert(offer);
    resources += offer->resources();
  }

  void removeOffer(Offer* offer)
  {
    CHECK(offers.find(offer) != offers.end());
    offers.erase(offer);
    resources -= offer->resources();
  }

  bool hasExecutor(const SlaveID& slaveId,
		   const ExecutorID& executorId)
  {
    return executors.contains(slaveId) &&
      executors[slaveId].contains(executorId);
  }

  void addExecutor(const SlaveID& slaveId,
		   const ExecutorInfo& executorInfo)
  {
    CHECK(!hasExecutor(slaveId, executorInfo.executor_id()));
    executors[slaveId][executorInfo.executor_id()] = executorInfo;

    // Update our resources to reflect running this executor.
    resources += executorInfo.resources();
  }

  void removeExecutor(const SlaveID& slaveId,
		      const ExecutorID& executorId)
  {
    if (hasExecutor(slaveId, executorId)) {
      // Update our resources to reflect removing this executor.
      resources -= executors[slaveId][executorId].resources();

      executors[slaveId].erase(executorId);
      if (executors[slaveId].size() == 0) {
	executors.erase(slaveId);
      }
    }
  }

  bool filters(Slave* slave, Resources resources)
  {
    // TODO: Implement other filters
    return slaveFilter.find(slave) != slaveFilter.end();
  }

  void removeExpiredFilters(double now)
  {
    foreachpair (Slave* slave, double removalTime, utils::copy(slaveFilter)) {
      if (removalTime != 0 && removalTime <= now) {
        slaveFilter.erase(slave);
      }
    }
  }

  const FrameworkID id;
  const FrameworkInfo info;

  UPID pid;

  bool active; // Turns false when framework is being removed.
  double registeredTime;
  double reregisteredTime;

  hashmap<TaskID, Task*> tasks;

  std::list<Task*> completedTasks;
  int maxCompletedTasks;

  hashset<Offer*> offers; // Active offers for framework.

  Resources resources; // Total resources (tasks + offers + executors).
  hashmap<SlaveID, hashmap<ExecutorID, ExecutorInfo> > executors;

  // Contains a time of unfiltering for each slave we've filtered,
  // or 0 for slaves that we want to keep filtered forever
  hashmap<Slave*, double> slaveFilter;
};

} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // __MASTER_HPP__
