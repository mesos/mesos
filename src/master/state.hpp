// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __MASTER_STATE_HPP__
#define __MASTER_STATE_HPP__

#include <deque>

#include <mesos/authorizer/authorizer.hpp>

#include <mesos/master/master.hpp>

#include <process/http.hpp>
#include <process/process.hpp>
#include <process/queue.hpp>

#include "master/flags.hpp"

namespace mesos {
namespace internal {
namespace master {

class StateProcess : public process::Process<StateProcess>
{
public:
  StateProcess(
      process::Queue<mesos::master::Event>&& events,
      const Flags& flags,
      const Option<mesos::Authorizer*>& authorizer = None())
    : process::ProcessBase("state"),
      events(std::move(events)),
      flags(flags),
      authorizer(authorizer) {}

protected:
  void initialize() override;

  process::Future<process::http::Response> graphql(
      const process::http::Request& request,
      const Option<process::http::authentication::Principal>& principal) const;

private:
  process::Queue<mesos::master::Event> events;
  const Flags flags;
  Option<mesos::Authorizer*> authorizer;
  mesos::master::Response::GetState state;

  hashmap<FrameworkID, std::deque<TaskID>> unreachableTaskIds;
  hashmap<FrameworkID, std::deque<TaskID>> completedTaskIds;
};

} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // __MASTER_STATE_HPP__
