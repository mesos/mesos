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

#include <deque>

#include <mesos/resources.hpp>

#include <mesos/master/master.hpp>

#include <process/future.hpp>
#include <process/http.hpp>
#include <process/loop.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/jsonify.hpp>
#include <stout/option.hpp>

#include "common/http.hpp"
#include "common/protobuf_utils.hpp"

#include "master/constants.hpp"
#include "master/flags.hpp"
#include "master/graphql.hpp"
#include "master/state.hpp"

namespace http = process::http;

using http::authentication::Principal;

using mesos::authorization::VIEW_EXECUTOR;
using mesos::authorization::VIEW_FRAMEWORK;
using mesos::authorization::VIEW_ROLE;
using mesos::authorization::VIEW_TASK;

using process::Continue;
using process::ControlFlow;
using process::loop;
using process::Future;
using process::Owned;

using std::string;

namespace mesos {
namespace internal {
namespace master {

using AgentInfo = SlaveInfo; // TODO(benh): Remove once we're using v1 protos.


template <typename T, typename F>
Option<T> remove(google::protobuf::RepeatedPtrField<T>* ts, F&& f)
{
  Option<T> option = None();

  auto iterator = ts->begin();

  while (iterator != ts->end()) {
    if (f(*iterator)) {
      option = *iterator;
      ts->erase(iterator);
      break;
    }
    iterator++;
  }

  return option;
}


void StateProcess::initialize()
{
  LOG(INFO) << "Initializing state";

  route(
      "/graphql",
      // TODO(benh): s/READONLY/READWRITE/ in the future?
      READONLY_HTTP_AUTHENTICATION_REALM,
      None(),
      [=](const http::Request& request,
          const Option<Principal>& principal) {
        return graphql(request, principal);
      });

  loop(
      self(),
      [=]() {
        return events.get();
      },
      [=](const mesos::master::Event& event) -> ControlFlow<Nothing> {
        using Agent = mesos::master::Response::GetAgents::Agent;
        using Framework = mesos::master::Response::GetFrameworks::Framework;

        switch (event.type()) {
          case mesos::master::Event::UNKNOWN:
            break;
          case mesos::master::Event::SUBSCRIBED: {
            state = event.subscribed().get_state();
            break;
          }
          case mesos::master::Event::TASK_ADDED: {
            auto task = event.task_added().task();
            state.mutable_get_tasks()->add_tasks()->CopyFrom(task);

            // Remove from `pending_tasks` if it exists.
            auto getTasks = state.mutable_get_tasks();
            auto pendingTasks = getTasks->mutable_pending_tasks();

            remove(pendingTasks, [&](const Task& t) {
              return t.framework_id() == task.framework_id() &&
                t.task_id() == task.task_id();
            });

            break;
          }
          case mesos::master::Event::TASK_UPDATED: {
            auto frameworkId = event.task_updated().framework_id();
            auto status = event.task_updated().status();
            auto state = event.task_updated().state();

            auto getTasks = this->state.mutable_get_tasks();

            // Find and remove from `pending_tasks`, `tasks`,
            // `unreachable_tasks`, and `completed_tasks`.
            auto pendingTasks = getTasks->mutable_pending_tasks();
            auto tasks = getTasks->mutable_tasks();
            auto unreachableTasks = getTasks->mutable_unreachable_tasks();
            auto completedTasks = getTasks->mutable_completed_tasks();

            Option<Task> task = remove(pendingTasks, [&](const Task& task) {
              return task.framework_id() == frameworkId &&
                task.task_id() == status.task_id();
            });

            if (task.isNone()) {
              task = remove(tasks, [&](const Task& task) {
                return task.framework_id() == frameworkId &&
                  task.task_id() == status.task_id();
              });
            }

            if (task.isNone()) {
              task = remove(unreachableTasks, [&](const Task& task) {
                return task.framework_id() == frameworkId &&
                  task.task_id() == status.task_id();
              });
            }

            if (task.isNone()) {
              task = remove(completedTasks, [&](const Task& task) {
                return task.framework_id() == frameworkId &&
                  task.task_id() == status.task_id();
              });
            }

            if (task.isSome()) {
              // Add the new status.
              task->add_statuses()->CopyFrom(status);

              // Update the state.
              task->set_state(state);

              if (state == TASK_UNREACHABLE) {
                getTasks->add_unreachable_tasks()->CopyFrom(task.get());

                // TODO(benh): Remove this task's ID first?
                auto taskIds = unreachableTaskIds[frameworkId];

                taskIds.push_back(task->task_id());

                if (taskIds.size() >
                    flags.max_unreachable_tasks_per_framework) {
                  remove(unreachableTasks, [&](const Task& task) {
                    return task.framework_id() == frameworkId &&
                      task.task_id() == taskIds.front();
                  });
                  taskIds.pop_front();
                }
              } else if (protobuf::isTerminalState(state)) {
                getTasks->add_completed_tasks()->CopyFrom(task.get());

                // TODO(benh): Remove this task's ID first?
                auto taskIds = completedTaskIds[frameworkId];

                taskIds.push_back(task->task_id());

                if (taskIds.size() >
                    flags.max_completed_tasks_per_framework) {
                  remove(completedTasks, [&](const Task& task) {
                    return task.framework_id() == frameworkId &&
                      task.task_id() == taskIds.front();
                  });
                  taskIds.pop_front();
                }
              } else {
                getTasks->add_tasks()->CopyFrom(task.get());
              }
            }

            break;
          }
          case mesos::master::Event::AGENT_ADDED: {
            auto agent = event.agent_added().mutable_agent();

            // Clear `offered_resources` because we don't get offer
            // updates.
            agent->clear_offered_resources();

            auto getAgents = state.mutable_get_agents();

            getAgents->add_agents()->CopyFrom(agent);

            // Remove from `recovered_agents` if present.
            auto recoveredAgents = getAgents->mutable_recovered_agents();

            remove(recoveredAgents, [&](const AgentInfo& agentInfo) {
              return agentInfo.id() == agent.agent_info().id();
            });

            break;
          }
          case mesos::master::Event::AGENT_REMOVED: {
            auto id = event.agent_removed().agent_id();

            auto getAgents = state.mutable_get_agents();

            // Remove from `agents`.
            auto agents = getAgents->mutable_agents();

            remove(agents, [&](const Agent& agent) {
              return agent.agent_info().id() == id;
            });

            // Remove from `recovered_agents` if present.
            auto recoveredAgents = getAgents->mutable_recovered_agents();

            remove(recoveredAgents, [&](const AgentInfo& agentInfo) {
              return agentInfo.id() == id;
            });


            break;
          }
          case mesos::master::Event::FRAMEWORK_ADDED: {
            auto framework = event.framework_added().mutable_framework();

            // Clear the `offers`, `inverse_offers`, and
            // `offered_resources` because we don't get offer updates.
            framework->clear_offers();
            framework->clear_inverse_offers();
            framework->clear_offered_resources();

            auto getFrameworks = state.mutable_get_frameworks();

            getFrameworks->add_frameworks()->CopyFrom(framework);

            break;
          }
          case mesos::master::Event::FRAMEWORK_UPDATED: {
            auto framework = event.framework_updated().mutable_framework();

            // Clear the `offers`, `inverse_offers`, and
            // `offered_resources` because we don't get offer updates.
            framework->clear_offers();
            framework->clear_inverse_offers();
            framework->clear_offered_resources();

            auto getFrameworks = state.mutable_get_frameworks();

            // Replace `framework` in existing `frameworks` by erasing
            // then adding. Erasing then adding versus overwriting in
            // `frameworks` has the benefit that if for some reason
            // the framework is not in `frameworks` we'll make sure it
            // gets added now.
            auto frameworks = getFrameworks->mutable_frameworks();

            remove(frameworks, [&](const Framework& f) {
              return f.framework_info().id() == framework.framework_info().id();
            });

            // Now add `framework`.
            getFrameworks->add_frameworks()->CopyFrom(framework);

            break;
          }
          case mesos::master::Event::FRAMEWORK_REMOVED: {
            auto id = event.framework_removed().framework_info().id();

            auto getFrameworks = state.mutable_get_frameworks();

            // Remove from `frameworks` and add to `completed_frameworks`.
            auto frameworks = getFrameworks->mutable_frameworks();

            Option<Framework> framework = remove(
                frameworks,
                [&](const Framework& f) {
                  return f.framework_info().id() == id;
                });

            // Move any active or unreachable tasks to completed too.
            // TODO(benh): Do we need to do this or can we assume
            // we'll get TASK_UPDATED for each task?

            auto getTasks = state->mutable_get_tasks();

            Option<Task> task = None();

            do {
              task = remove(tasks, [&](const Task& task) {
                return task.framework_id() == id;
              });
              getTasks->add_completed_tasks()->CopyFrom(task);

              cant_add_more_than_flags;

              
            } while (task.isSome());

            do {
              task = remove(unreachableTasks, [&](const Task& task) {
                return task.framework_id() == id;
              });
              getTasks->add_completed_tasks()->CopyFrom(task);

              cant_add_more_than_flags;
              

            } while (task.isSome());

            // Add to `completed_frameworks`.
            if (framework.isSome()) {
              getFrameworks->add_completed_frameworks()->CopyFrom(framework.get());

              if (getFrameworks->completed_frameworks_size() >
                  flags.max_completed_frameworks) {
                auto completedFrameworks =
                  getFrameworks->mutable_completed_frameworks();

                completedFrameworks->erase(completedFrameworks->begin());

                do {
                  task = remove(completedTasks, [&](const Task& task) {
                    return task.framework_id() == id;
                  });
                } while (task.isSome());
              }
            }

            break;
          }
          case mesos::master::Event::HEARTBEAT:
            break;
        }
        return Continue();
      });
}


Future<http::Response> StateProcess::graphql(
    const http::Request& request,
    const Option<Principal>& principal) const
{
  if (request.method != "POST") {
    return http::MethodNotAllowed({"POST"});
  }

  // TODO(greggomann): Remove this check once the `Principal` type is used in
  // `ReservationInfo`, `DiskInfo`, and within the master's `principals` map.
  // See MESOS-7202.
  if (principal.isSome() && principal->value.isNone()) {
    return http::Forbidden(
        "The request's authenticated principal contains claims, but no value "
        "string. The master currently requires that principals have a value");
  }

  return ObjectApprovers::create(
      authorizer,
      principal,
      {VIEW_FRAMEWORK, VIEW_TASK, VIEW_EXECUTOR, VIEW_ROLE})
    .then(defer(
        self(),
        [=](const Owned<ObjectApprovers>& approvers) -> http::Response {
          // TODO(benh): Get all of the body if we have a pipe.
          CHECK_EQ(http::Request::BODY, request.type);

          Option<Error> error = None();

          http::Response response = http::OK(jsonify([&](JSON::ObjectWriter* writer) {
            writer->field("data", [&](JSON::ObjectWriter* writer) {
              error = graphql::execute(request.body, state, writer);
            });
          }),
          request.url.query.get("jsonp"));

          if (error.isNone()) {
            return response;
          }

          return http::OK(jsonify([&](JSON::ObjectWriter* writer) {
            writer->field("errors", [&](JSON::ArrayWriter* writer) {
              writer->element(error->message);
            });
          }),
          request.url.query.get("jsonp"));
        }));
}

void StateProcess::addFramework()
{

}


void StateProcess::frameworkCompleted(const FrameworkID& frameworkId)
{
  auto getFrameworks = state.mutable_get_frameworks();

  // Remove from `frameworks` and add to `completed_frameworks`.
  auto frameworks = getFrameworks->mutable_frameworks();

  Option<Framework> framework = remove(
      frameworks,
      [&](const Framework& framework) {
        return framework.framework_info().id() == frameworkId;
      });

  // NOTE: we assume that at this point WE WILL NOT get any more
  // updates about tasks of this framework because it has been removed
  // and therefore we manually "complete" all remaining tasks
  // (unreachable ones too).

  auto getTasks = state->mutable_get_tasks();

  Option<Task> task = None();

  do {
    task = remove(tasks, [&](const Task& task) {
        return task.framework_id() == id;
      });
    getTasks->add_completed_tasks()->CopyFrom(task);

    cant_add_more_than_flags;

              
            } while (task.isSome());

            do {
              task = remove(unreachableTasks, [&](const Task& task) {
                return task.framework_id() == id;
              });
              getTasks->add_completed_tasks()->CopyFrom(task);

              cant_add_more_than_flags;
              

            } while (task.isSome());

            // Add to `completed_frameworks`.
            if (framework.isSome()) {
              getFrameworks->add_completed_frameworks()->CopyFrom(framework.get());

              if (getFrameworks->completed_frameworks_size() >
                  flags.max_completed_frameworks) {
                auto completedFrameworks =
                  getFrameworks->mutable_completed_frameworks();

                completedFrameworks->erase(completedFrameworks->begin());

                do {
                  task = remove(completedTasks, [&](const Task& task) {
                    return task.framework_id() == id;
                  });
                } while (task.isSome());
              }
            }
}


void StateProcess::addTask()
{

}


void StateProcess::completeTasks(const FrameworkID& frameworkId)
{
  Option<Task> task = None();

  do {
    task = remove(tasks, [&](const Task& task) {
      return task.framework_id() == frameworkId;
    });

    if (task.isSome()) {
      taskCompleted(task.get());
    }
  } while (task.isSome());

  do {
    task = remove(unreachableTasks, [&](const Task& task) {
      return task.framework_id() == frameworkId;
    });

    if (task.isSome()) {
      taskCompleted(task.get());
    }
  } while (task.isSome());

}


void StateProcess::taskUnreachable(const Task& task)
{
  auto getTasks = state.mutable_get_tasks();

  getTasks->add_unreachable_tasks()->CopyFrom(task);

  // TODO(benh): Is it possible that we'll get notified more than once
  // that the task is unreaachable? If so, we should remove this task
  // before adding it a second time.
  auto taskIds = unreachableTaskIds[task.framework_id()];

  taskIds.push_back(task->task_id());

  if (taskIds.size() > flags.max_unreachable_tasks_per_framework) {
    // Remove the oldest (first-in) unreachable task for the framework
    // represented by the task ID at the front of the task ID deque.
    remove(unreachableTasks, [&](const Task& t) {
      return t.framework_id() == task.framework_id() &&
        t.task_id() == taskIds.front();
      });

    // And pop the front of the deque!
    taskIds.pop_front();
}


void StateProcess::taskCompleted(const Task& task)
{
  auto getTasks = state.mutable_get_tasks();

  getTasks->add_completed_tasks()->CopyFrom(task);

  // TODO(benh): Is it possible that we'll get notified more than once
  // that the task has completed? If so, we should remove this task
  // before adding it a second time.
  auto taskIds = completedTaskIds[task.framework_id()];

  taskIds.push_back(task->task_id());

  if (taskIds.size() > flags.max_completed_tasks_per_framework) {
    // Remove the oldest (first-in) completed task for the framework
    // represented by the task ID at the front of the task ID deque.
    remove(completedTasks, [&](const Task& t) {
      return t.framework_id() == task.framework_id() &&
        t.task_id() == taskIds.front();
      });

    // And pop the front of the deque!
    taskIds.pop_front();
}

} // namespace master {
} // namespace internal {
} // namespace mesos {
