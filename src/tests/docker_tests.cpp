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

#include <gtest/gtest.h>

#include <process/future.hpp>
#include <process/gtest.hpp>

#include <stout/option.hpp>
#include <stout/gtest.hpp>

#include "docker/docker.hpp"

#include "mesos/resources.hpp"

#include "tests/flags.hpp"

using namespace mesos;
using namespace mesos::internal;

using process::Future;

using std::list;
using std::string;


// This test tests the functionality of the
// docker's interfaces.
TEST(DockerTest, DOCKER_interface)
{
  string containerName = "mesos-docker-test";
  Resources resources = Resources::parse("cpus:1;mem:512").get();
  Docker docker(tests::flags.docker);

  // Cleaning up the container first.
  Future<Option<int> > status = docker.rm(containerName, true);
  AWAIT_READY(status);
  ASSERT_SOME(status.get());

  // Verify that we do not see the container.
  Future<list<Docker::Container> > containers = docker.ps(true);
  AWAIT_READY(containers);
  foreach (const Docker::Container& container, containers.get()) {
    EXPECT_NE("/" + containerName, container.name());
  }

  // Start the container.
  status = docker.run("busybox", "sleep 120", containerName, resources);
  AWAIT_READY(status);
  ASSERT_SOME(status.get());

  // Should be able to see the container now.
  containers = docker.ps();
  AWAIT_READY(containers);
  bool found = false;
  foreach (const Docker::Container& container, containers.get()) {
    if ("/" + containerName == container.name()) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  Future<Docker::Container> container = docker.inspect(containerName);
  AWAIT_READY(container);

  // Test some fields of the container.
  EXPECT_NE("", container.get().id());
  EXPECT_EQ("/" + containerName, container.get().name());
  EXPECT_SOME(container.get().pid());

  // Kill the container.
  status = docker.kill(containerName);
  AWAIT_READY(status);
  ASSERT_SOME(status.get());

  // Now, the container should not appear in the result of ps().
  // But it should appear in the result of ps(true).
  containers = docker.ps();
  AWAIT_READY(containers);
  foreach (const Docker::Container& container, containers.get()) {
    EXPECT_NE("/" + containerName, container.name());
  }

  containers = docker.ps(true);
  AWAIT_READY(containers);
  found = false;
  foreach (const Docker::Container& container, containers.get()) {
    if ("/" + containerName == container.name()) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  // Check the container's info, both id and name should remain
  // the same since we haven't removed it, but the pid should be none
  // since it's not running.
  container = docker.inspect(containerName);
  AWAIT_READY(container);

  EXPECT_NE("", container.get().id());
  EXPECT_EQ("/" + containerName, container.get().name());
  EXPECT_NONE(container.get().pid());

  // Remove the container.
  status = docker.rm(containerName);
  AWAIT_READY(status);
  ASSERT_SOME(status.get());

  // Should not be able to inspect the container.
  container = docker.inspect(containerName);
  AWAIT_FAILED(container);

  // Also, now we should not be able to see the container
  // by invoking ps(true).
  containers = docker.ps(true);
  AWAIT_READY(containers);
  foreach (const Docker::Container& container, containers.get()) {
    EXPECT_NE("/" + containerName, container.name());
  }

  // Start the container again, this time we will do a "rm -f"
  // directly, instead of killing and rm.
  //
  // First, Invoke docker.run()
  status = docker.run("busybox", "sleep 120", containerName, resources);
  AWAIT_READY(status);
  ASSERT_SOME(status.get());

  // Verify that the container is there.
  containers = docker.ps();
  AWAIT_READY(containers);
  found = false;
  foreach (const Docker::Container& container, containers.get()) {
    if ("/" + containerName == container.name()) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  // Then do a "rm -f".
  status = docker.rm(containerName, true);
  AWAIT_READY(status);
  ASSERT_SOME(status.get());

  // Verify that the container is totally removed,
  // that is we can't find it by ps() or ps(true).
  containers = docker.ps();
  AWAIT_READY(containers);
  foreach (const Docker::Container& container, containers.get()) {
    EXPECT_NE("/" + containerName, container.name());
  }
  containers = docker.ps(true);
  AWAIT_READY(containers);
  foreach (const Docker::Container& container, containers.get()) {
    EXPECT_NE("/" + containerName, container.name());
  }
}
