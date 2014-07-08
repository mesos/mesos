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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <process/future.hpp>
#include <process/subprocess.hpp>

#include "tests/flags.hpp"
#include "tests/mesos.hpp"

#include "slave/containerizer/docker.hpp"
#include "slave/slave.hpp"
#include "slave/state.hpp"


using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::slave::state;
using namespace mesos::internal::tests;

using mesos::internal::master::Master;

using mesos::internal::slave::Slave;
using mesos::internal::slave::DockerContainerizer;

using process::Future;
using process::PID;

using std::vector;
using std::list;
using std::string;

using testing::_;
using testing::DoDefault;
using testing::Invoke;
using testing::Return;

class DockerContainerizerTest : public MesosTest {};

class MockDockerContainerizer : public DockerContainerizer {
public:
  MockDockerContainerizer(
      const slave::Flags& flags,
      bool local,
      const Docker& docker)
    : DockerContainerizer(flags, local, docker)
  {
    EXPECT_CALL(*this, launch(_, _, _, _, _, _, _, _))
      .WillRepeatedly(Invoke(this, &MockDockerContainerizer::_launch));
  }

  MOCK_METHOD8(
      launch,
      process::Future<bool>(
          const ContainerID&,
          const TaskInfo&,
          const ExecutorInfo&,
          const std::string&,
          const Option<std::string>&,
          const SlaveID&,
          const process::PID<slave::Slave>&,
          bool checkpoint));

  // Default 'launch' implementation (necessary because we can't just
  // use &DockerContainerizer::launch with 'Invoke').
  process::Future<bool> _launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const string& directory,
      const Option<string>& user,
      const SlaveID& slaveId,
      const PID<Slave>& slavePid,
      bool checkpoint)
  {
    return DockerContainerizer::launch(
        containerId,
        taskInfo,
        executorInfo,
        directory,
        user,
        slaveId,
        slavePid,
        checkpoint);
  }
};


TEST_F(DockerContainerizerTest, DOCKER_Launch)
{
  Try<PID<Master> > master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();

  Docker docker(tests::flags.docker);

  MockDockerContainerizer dockerContainerizer(flags, true, docker);

  Try<PID<Slave> > slave = StartSlave(&dockerContainerizer);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master.get(), DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer> > offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(frameworkId);

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  const Offer& offer = offers.get()[0];

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->CopyFrom(offer.slave_id());
  task.mutable_resources()->CopyFrom(offer.resources());

  CommandInfo command;
  CommandInfo::ContainerInfo* containerInfo = command.mutable_container();
  containerInfo->set_image("docker://busybox");
  command.set_value("sleep 120");

  task.mutable_command()->CopyFrom(command);

  vector<TaskInfo> tasks;
  tasks.push_back(task);

  Future<ContainerID> containerId;
  EXPECT_CALL(dockerContainerizer, launch(_, _, _, _, _, _, _, _))
    .WillOnce(DoAll(FutureArg<0>(&containerId),
                    Invoke(&dockerContainerizer,
                           &MockDockerContainerizer::_launch)));

  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillRepeatedly(DoDefault());

  driver.launchTasks(offers.get()[0].id(), tasks);

  AWAIT_READY_FOR(containerId, Seconds(60));
  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());

  Future<list<Docker::Container> > containers =
    docker.ps(true, slave::DOCKER_NAME_PREFIX);

  AWAIT_READY(containers);

  ASSERT_TRUE(containers.get().size() > 0);

  bool foundContainer = false;
  string expectedName = slave::DOCKER_NAME_PREFIX + containerId.get().value();

  foreach (const Docker::Container& container, containers.get()) {
    // Docker inspect name contains an extra slash in the beginning.
    if (strings::contains(container.name(), expectedName)) {
      foundContainer = true;
      break;
    }
  }

  ASSERT_TRUE(foundContainer);

  dockerContainerizer.destroy(containerId.get());

  driver.stop();
  driver.join();

  Shutdown();
}


// This test tests DockerContainerizer::usage().
TEST_F(DockerContainerizerTest, DOCKER_Usage)
{
  Try<PID<Master> > master = StartMaster();
  ASSERT_SOME(master);

  slave::Flags flags = CreateSlaveFlags();
  flags.resources = Option<string>("cpus:2;mem:1024");

  Docker docker(tests::flags.docker);

  MockDockerContainerizer dockerContainerizer(flags, true, docker);

  Try<PID<Slave> > slave = StartSlave(&dockerContainerizer, flags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
    &sched, DEFAULT_FRAMEWORK_INFO, master.get(), DEFAULT_CREDENTIAL);

  Future<FrameworkID> frameworkId;
  EXPECT_CALL(sched, registered(&driver, _, _))
    .WillOnce(FutureArg<1>(&frameworkId));

  Future<vector<Offer> > offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(frameworkId);

  AWAIT_READY(offers);
  EXPECT_NE(0u, offers.get().size());

  const Offer& offer = offers.get()[0];

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->CopyFrom(offer.slave_id());
  task.mutable_resources()->CopyFrom(offer.resources());

  CommandInfo command;
  CommandInfo::ContainerInfo* containerInfo = command.mutable_container();
  containerInfo->set_image("docker://busybox");

  // Run a CPU intensive command, so we can measure utime and stime later.
  command.set_value("dd if=/dev/zero of=/dev/null");

  task.mutable_command()->CopyFrom(command);

  vector<TaskInfo> tasks;
  tasks.push_back(task);

  Future<ContainerID> containerId;
  EXPECT_CALL(dockerContainerizer, launch(_, _, _, _, _, _, _, _))
    .WillOnce(DoAll(FutureArg<0>(&containerId),
                    Invoke(&dockerContainerizer,
                           &MockDockerContainerizer::_launch)));

  Future<TaskStatus> statusRunning;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&statusRunning))
    .WillRepeatedly(DoDefault());

  driver.launchTasks(offers.get()[0].id(), tasks);

  AWAIT_READY_FOR(containerId, Seconds(60));
  AWAIT_READY_FOR(statusRunning, Seconds(60));
  EXPECT_EQ(TASK_RUNNING, statusRunning.get().state());

  // Verify the usage.
  ResourceStatistics statistics;
  Duration waited = Duration::zero();
  do {
    Future<ResourceStatistics> usage =
      dockerContainerizer.usage(containerId.get());
    AWAIT_READY(usage);

    statistics = usage.get();

    if (statistics.cpus_user_time_secs() > 0 &&
        statistics.cpus_system_time_secs() > 0) {
      break;
    }

    os::sleep(Milliseconds(200));
    waited += Milliseconds(200);
  } while (waited < Seconds(3));

  EXPECT_EQ(2, statistics.cpus_limit());
  EXPECT_EQ(Gigabytes(1).bytes(), statistics.mem_limit_bytes());
  EXPECT_LT(0, statistics.cpus_user_time_secs());
  EXPECT_LT(0, statistics.cpus_system_time_secs());

  Future<containerizer::Termination> termination =
    dockerContainerizer.wait(containerId.get());

  dockerContainerizer.destroy(containerId.get());

  AWAIT_READY(termination);

  // Usage() should fail again since the container is destroyed
  Future<ResourceStatistics> usage =
    dockerContainerizer.usage(containerId.get());
  AWAIT_FAILED(usage);

  driver.stop();
  driver.join();

  Shutdown();
}


TEST_F(DockerContainerizerTest, DOCKER_Recover)
{
  slave::Flags flags = CreateSlaveFlags();

  Docker docker(tests::flags.docker);

  MockDockerContainerizer dockerContainerizer(flags, true, docker);

  ContainerID containerId;
  containerId.set_value("c1");
  ContainerID reapedContainerId;
  reapedContainerId.set_value("c2");

  Resources resources = Resources::parse("cpus:1;mem:512").get();

  Future<Option<int> > d1 =
    docker.run(
        "busybox",
        "sleep 360",
        slave::DOCKER_NAME_PREFIX + stringify(containerId),
        resources);

  Future<Option<int> > d2 =
    docker.run(
        "busybox",
        "sleep 360",
        slave::DOCKER_NAME_PREFIX + stringify(reapedContainerId),
        resources);

  AWAIT_READY(d1);
  AWAIT_READY(d2);

  SlaveState slaveState;
  FrameworkState frameworkState;

  ExecutorID execId;
  execId.set_value("e1");

  ExecutorState execState;
  ExecutorInfo execInfo;
  execState.info = execInfo;
  execState.latest = containerId;

  Try<process::Subprocess> wait =
    process::subprocess(
        "docker wait " +
        slave::DOCKER_NAME_PREFIX +
        stringify(containerId));

  ASSERT_SOME(wait);

  Try<process::Subprocess> reaped =
    process::subprocess(
        "docker wait " +
        slave::DOCKER_NAME_PREFIX +
        stringify(reapedContainerId));

  ASSERT_SOME(reaped);

  FrameworkID frameworkId;

  RunState runState;
  runState.id = containerId;
  runState.forkedPid = wait.get().pid();
  execState.runs.put(containerId, runState);
  frameworkState.executors.put(execId, execState);

  slaveState.frameworks.put(frameworkId, frameworkState);

  Future<Nothing> recover = dockerContainerizer.recover(slaveState);

  AWAIT_READY(recover);

  Future<containerizer::Termination> termination =
    dockerContainerizer.wait(containerId);

  ASSERT_FALSE(termination.isFailed());

  AWAIT_FAILED(dockerContainerizer.wait(reapedContainerId));

  dockerContainerizer.destroy(containerId);

  AWAIT_READY(termination);

  AWAIT_READY(reaped.get().status());
}
