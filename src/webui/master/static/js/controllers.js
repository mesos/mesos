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

import { request, stream } from '@dcos/mesos-client';

(function() {
  'use strict';

  var mesosApp = angular.module('mesos');

  // Returns the URL prefix for an agent, there are two cases
  // to consider:
  //
  //   (1) Some endpoints for the agent process itself require
  //       the agent PID.name (processId) in the path, this is
  //       to ensure that the webui works correctly when running
  //       against mesos-local or other instances of multiple agents
  //       running within the same "host:port":
  //
  //         //hostname:port/slave(1)
  //         //hostname:port/slave(2)
  //         ...
  //
  //   (2) Some endpoints for other components in the agent
  //       do not require the agent PID.name in the path, since
  //       a single endpoint serves multiple agents within the
  //       same process. In this case we just return:
  //
  //         //hostname:port
  //
  // Note that there are some clashing issues in mesos-local
  // (e.g., hosting '/slave/log' for each agent log, we don't
  // namespace metrics within '/metrics/snapshot', etc).
  function agentURLPrefix(agent, includeProcessId) {
    var port = agent.pid.substring(agent.pid.lastIndexOf(':') + 1);
    var processId = agent.pid.substring(0, agent.pid.indexOf('@'));

    var url = '//' + agent.agent_info.hostname + ':' + port;

    if (includeProcessId) {
      url += '/' + processId;
    }

    return url;
  }

  // Invokes the pailer, building the endpoint URL with the specified urlPrefix
  // and path.
  function pailer(urlPrefix, path, window_title) {
    var url = urlPrefix + '/files/read?path=' + path;

    // The randomized `storageKey` is removed from `localStorage` once the
    // pailer window loads the URL into its `sessionStorage`, therefore
    // the probability of collisions is low and we do not use a uuid.
    var storageKey = Math.random().toString(36).substr(2, 8);

    // Store the target URL in `localStorage` which is
    // accessed by the pailer window when opened.
    localStorage.setItem(storageKey, url);

    var pailer =
      window.open('static/pailer.html', storageKey, 'width=580px, height=700px');

    // Need to use window.onload instead of document.ready to make
    // sure the title doesn't get overwritten.
    pailer.onload = function() {
      pailer.document.title = window_title;
    };
  }


  function updateInterval(num_agents) {
    // TODO(bmahler): Increasing the update interval for large clusters
    // is done purely to mitigate webui performance issues. Ideally we can
    // keep a consistently fast rate for updating statistical information.
    // For the full system state updates, it may make sense to break
    // it up using pagination and/or splitting the endpoint.
    if (num_agents < 500) {
      return 10000;
    } else if (num_agents < 1000) {
      return 20000;
    } else if (num_agents < 5000) {
      return 60000;
    } else if (num_agents < 10000) {
      return 120000;
    } else if (num_agents < 15000) {
      return 240000;
    } else if (num_agents < 20000) {
      return 480000;
    } else {
      return 960000;
    }
  }

  // Set the task sandbox directory for use by the WebUI.
  function setTaskSandbox(executor) {
    _.each(
        [executor.tasks, executor.queued_tasks, executor.completed_tasks],
        function(tasks) {
      _.each(tasks, function(task) {
        if (executor.type === 'DEFAULT') {
          task.directory = executor.directory + '/tasks/' + task.id;
        } else {
          task.directory = executor.directory;
        }
      });
    });
  }

  var isStateTerminal = function(taskState) {
    var terminalStates = [
      'TASK_ERROR',
      'TASK_FAILED',
      'TASK_FINISHED',
      'TASK_KILLED',
      'TASK_LOST',
      'TASK_DROPPED',
      'TASK_GONE',
      'TASK_GONE_BY_OPERATOR'
    ];
    return terminalStates.indexOf(taskState) > -1;
  };

  var setTaskMetadata = function(task) {
    task.role = task.resources[0].allocation_info.role;

    if (!task.executor_id) {
      task.executor_id = task.task_id;
    }

    // Task might not have any statuses if it's just been added and we
    // received a streamed event TASK_ADDED. We add one by default so
    // the rest of the code can expect it to be present.
    if (!_.has(task, 'statuses')) {
      task.statuses = [];
    }

    if (task.statuses.length > 0) {
      var firstStatus = task.statuses[0];
      if (!isStateTerminal(firstStatus.state)) {
        task.start_time = firstStatus.timestamp * 1000;
      }
      var lastStatus = task.statuses[task.statuses.length - 1];
      if (isStateTerminal(task.state)) {
        task.finish_time = lastStatus.timestamp * 1000;
      }
      task.healthy = lastStatus.healthy;
    }
  };

  function safeApply($scope, f) {
    return ($scope.$$phase || $scope.$root.$$phase)
      ? f()
      : $scope.$apply(f);
  }

  function subscribed($scope, state) {
    return safeApply($scope, function() {
      // Don't do anything if the state hasn't changed.
      if ($scope.state == state) {
        return;
      }

      $scope.state = state;

      // Reset the maps and arrays.
      $scope.agents = {};
      $scope.frameworks = {};
      $scope.completed_frameworks = {};
      $scope.offers = {};
      $scope.active_tasks = [];
      $scope.unreachable_tasks = [];
      $scope.completed_tasks = [];

      // Update the stats.
      $scope.total_cpus = 0;
      $scope.total_gpus = 0;
      $scope.total_mem = 0;
      $scope.total_disk = 0;
      $scope.allocated_cpus = 0;
      $scope.allocated_gpus = 0;
      $scope.allocated_mem = 0;
      $scope.allocated_disk = 0;

      $scope.activated_agents = 0;
      $scope.deactivated_agents = 0;

      // NOTE: ordering is important here! Must do agents and
      // frameworks and completed frameworks before tasks so that when
      // we do the tasks we properly update the agents and frameworks.

      _.each($scope.state.get_agents.agents, function(agent) {
        agentAdded($scope, agent);
      });

      _.each($scope.state.get_frameworks.frameworks, function(framework) {
        frameworkAdded($scope, framework);
      });

      _.each($scope.state.get_frameworks.completed_frameworks, function(framework) {
        frameworkAdded($scope, framework);
        frameworkRemoved($scope, framework.framework_info);
      });

      // TODO(benh): Do something with `$scope.state.get_tasks.pending_tasks`?

      _.each($scope.state.get_tasks.tasks, function(task) {
        taskAdded($scope, task);
      });

      if (_.has($scope.state.get_tasks, 'unreachable_tasks')) {
        _.each($scope.state.get_tasks.unreachable_tasks, function(task) {
          setTaskMetadata(task);

          var framework = _.has($scope.frameworks, task.framework_id.value)
              ? $scope.frameworks[task.framework_id.value]
              : undefined;

          framework.unreachable_tasks = framework.unreachable_tasks.concat(task);

          // TODO(benh): Update `allocation_*` of framework and agent.

          $scope.unreachable_tasks = $scope.unreachable_tasks.concat(task);
        });
      }

      _.each($scope.state.get_tasks.completed_tasks, function(task) {
        setTaskMetadata(task);

        var framework = undefined;

        if (_.has($scope.frameworks, task.framework_id.value)) {
          framework = $scope.frameworks[task.framework_id.value];
        } else if (_.has($scope.completed_frameworks, task.framework_id.value)) {
          framework = $scope.completed_frameworks[task.framework_id.value];
        }

        if (!_.isUndefined(framework)) {
          framework.completed_tasks = framework.completed_tasks.concat(task);
          switch (task.state) {
          case "TASK_FINISHED": framework.finished_tasks++; break;
          case "TASK_KILLED": framework.killed_tasks++; break;
          case "TASK_FAILED": framework.failed_tasks++; break;
          case "TASK_LOST": framework.lost_tasks++; break;
          }
        }

        $scope.completed_tasks = $scope.completed_tasks.concat(task);
      });

      $scope.idle_cpus = $scope.total_cpus - $scope.allocated_cpus;
      $scope.idle_gpus = $scope.total_gpus - $scope.allocated_gpus;
      $scope.idle_mem = $scope.total_mem - $scope.allocated_mem;
      $scope.idle_disk = $scope.total_disk - $scope.allocated_disk;
    });
  }

  function taskAdded($scope, task) {
    return safeApply($scope, function() {
      setTaskMetadata(task);

      $scope.active_tasks = $scope.active_tasks.concat(task);

      // TODO(benh): Do we need to be so defensive?
      var framework = _.has($scope.frameworks, task.framework_id.value)
          ? $scope.frameworks[task.framework_id.value]
          : undefined;

      if (_.isUndefined(framework)) {
        return;
      }

      framework.tasks = framework.tasks.concat(task);

      switch (task.state) {
        case "TASK_STAGING": framework.staging_tasks++; break;
        case "TASK_STARTING": framework.starting_tasks++; break;
        case "TASK_RUNNING": framework.running_tasks++; break;
        case "TASK_KILLING": framework.killing_tasks++; break;
      }

      _.each(task.resources, function(resource) {
        if (resource.name === "cpus") {
          framework.allocated_cpus += resource.scalar.value;
        } else if (resource.name === "gpus") {
          framework.allocated_gpus = resource.scalar.value;
        } else if (resource.name === "mem") {
          framework.allocated_mem = resource.scalar.value;
        } else if (resource.name === "disk") {
          framework.allocated_disk += resource.scalar.value;
        }
      });

      // TODO(benh): Do we need to be so defensive?
      var agent = _.has($scope.agents, task.agent_id.value)
          ? $scope.agents[task.agent_id.value]
          : undefined;

      if (_.isUndefined(agent)) {
        return;
      }

      _.each(task.resources, function(resource) {
        if (resource.name === "cpus") {
          agent.allocated_cpus += resource.scalar.value;
        } else if (resource.name === "gpus") {
          agent.allocated_gpus = resource.scalar.value;
        } else if (resource.name === "mem") {
          agent.allocated_mem = resource.scalar.value;
        } else if (resource.name === "disk") {
          agent.allocated_disk += resource.scalar.value;
        }
      });
    });
  }

  function findFramework($scope, framework_id) {
    if (_.has($scope.frameworks, framework_id.value)) {
      return $scope.frameworks[framework_id.value];
    } else if (_.has($scope.completed_frameworks, framework_id.value)) {
      return $scope.completed_frameworks[framework_id.value];
    }
    return undefined;
  }

  function findTask($scope, framework, task_id) {
    return _.find(framework.tasks, function (task) {
      return task.task_id.value === task_id.value;
    });
  }

  function taskUpdated($scope, framework_id, status, state) {
    return safeApply($scope, function() {
      var framework = findFramework($scope, framework_id);

      if (_.isUndefined(framework)) {
        return;
      }

      var task = findTask($scope, framework, status.task_id);

      if (_.isUndefined(task)) {
        return;
      }

      framework.tasks = _.filter(framework.tasks, function(t) {
        return t.task_id.value !== task.task_id.value;
      });

      framework.unreachable_tasks = _.filter(framework.unreachable_tasks, function(t) {
        return t.task_id.value !== task.task_id.value;
      });

      framework.completed_tasks = _.filter(framework.completed_tasks, function(t) {
        return t.task_id.value !== task.task_id.value;
      });

      $scope.active_tasks = _.filter($scope.active_tasks, function(t) {
        return t.task_id.value !== task.task_id.value;
      });

      $scope.unreachable_tasks = _.filter($scope.unreachable_tasks, function(t) {
        return t.task_id.value !== task.task_id.value;
      });

      $scope.completed_tasks = _.filter($scope.completed_tasks, function(t) {
        return t.task_id.value !== task.task_id.value;
      });

      if (state === "TASK_UNREACHABLE") {
        framework.unreachable_tasks = framework.unreachable_tasks.concat(task);
        $scope.unreachable_tasks = $scope.unreachable_tasks.concat(task);
      } else if (isStateTerminal(state)) {
        framework.completed_tasks = framework.completed_tasks.concat(task);
        $scope.completed_tasks = $scope.completed_tasks.concat(task);
      } else {
        framework.tasks = framework.tasks.concat(task);
        $scope.active_tasks = $scope.active_tasks.concat(task);
      }

      switch (task.state) {
        case "TASK_STAGING": framework.staging_tasks--; break;
        case "TASK_STARTING": framework.starting_tasks--; break;
        case "TASK_RUNNING": framework.running_tasks--; break;
        case "TASK_KILLING": framework.killing_tasks--; break;
        case "TASK_FINISHED": framework.finished_tasks--; break;
        case "TASK_KILLED": framework.killed_tasks--; break;
        case "TASK_FAILED": framework.failed_tasks--; break;
        case "TASK_LOST": framework.lost_tasks--; break;
      }

      task.statuses = task.statuses.concat(status);
      task.state = state;

      switch (task.state) {
        case "TASK_STAGING": framework.staging_tasks++; break;
        case "TASK_STARTING": framework.starting_tasks++; break;
        case "TASK_RUNNING": framework.running_tasks++; break;
        case "TASK_KILLING": framework.killing_tasks++; break;
        case "TASK_FINISHED": framework.finished_tasks++; break;
        case "TASK_KILLED": framework.killed_tasks++; break;
        case "TASK_FAILED": framework.failed_tasks++; break;
        case "TASK_LOST": framework.lost_tasks++; break;
      }

      // TODO(benh): Update framework and agent `allocated_*`.
    });
  }

  function agentAdded($scope, agent) {
    return safeApply($scope, function() {
      if (agent.active) {
        $scope.activated_agents += 1;
      } else {
        $scope.deactivated_agents += 1;
      }

      agent.total_cpus = 0;
      agent.total_gpus = 0;
      agent.total_mem = 0;
      agent.total_disk = 0;

      agent.allocated_cpus = 0;
      agent.allocated_gpus = 0;
      agent.allocated_mem = 0;
      agent.allocated_disk = 0;

      _.each(agent.total_resources, function(resource) {
        if (resource.name == "cpus") {
          agent.total_cpus = resource.scalar.value;
        } else if (resource.name == "gpus") {
          agent.total_gpus = resource.scalar.value;
        } else if (resource.name == "mem") {
          agent.total_mem = resource.scalar.value;
        } else if (resource.name == "disk") {
          agent.total_disk = resource.scalar.value;
        }
      });

      $scope.total_cpus += agent.total_cpus;
      $scope.total_gpus += agent.total_gpus;
      $scope.total_mem += agent.total_mem;
      $scope.total_disk += agent.total_disk;

      $scope.agents = _.clone($scope.agents);
      $scope.agents[agent.agent_info.id.value] = agent;
    });
  }

  function agentRemoved($scope, agent_id) {
    return safeApply($scope, function() {
      // TODO(benh): Do we need to be so defensive?
      var agent = _.has($scope.agents, agent_id.value)
          ? $scope.agents[agent_id.value]
          : undefined;

      if (!_.isUndefined(agent)) {
        if (agent.active) {
          $scope.activated_agents -= 1;
        } else {
          $scope.deactivated_agents -= 1;
        }

        $scope.total_cpus -= agent.total_cpus;
        $scope.total_gpus -= agent.total_gpus;
        $scope.total_mem -= agent.total_mem;
        $scope.total_disk -= agent.total_disk;

        $scope.agents = _.omit($scope.agents, agent_id.value);
      }
    });
  }

  function frameworkAdded($scope, framework) {
    return safeApply($scope, function() {
      // Fill in the `roles` field for non-MULTI_ROLE schedulers.
      if (framework.framework_info.role) {
        framework.framework_info.roles = [framework.framework_info.role];
      }

      framework.tasks = [];
      framework.unreachable_tasks = [];
      framework.completed_tasks = [];

      framework.allocated_cpus = 0;
      framework.allocated_gpus = 0;
      framework.allocated_mem = 0;
      framework.allocated_disk = 0;

      // TODO(bmahler): Add per-framework metrics to the master so that
      // the webui does not need to loop over all tasks!
      framework.staging_tasks = 0;
      framework.starting_tasks = 0;
      framework.running_tasks = 0;
      framework.killing_tasks = 0;
      framework.finished_tasks = 0;
      framework.killed_tasks = 0;
      framework.failed_tasks = 0;
      framework.lost_tasks = 0;

      _.each($scope.active_tasks, function(task) {
        if (task.framework_id.value === framework.framework_info.id.value) {
          framework.tasks = framework.tasks.concat(task);
          _.each(task.resources, function(resource) {
            if (resource.name === "cpus") {
              framework.allocated_cpus += resource.scalar.value;
            } else if (resource.name === "gpus") {
              framework.allocated_gpus += resource.scalar.value;
            } else if (resource.name === "mem") {
              framework.allocated_mem += resource.scalar.value;
            } else if (resource.name === "disk") {
              framework.allocated_disk += resource.scalar.value;
            }
          });
          switch (task.state) {
            case "TASK_STAGING": framework.staging_tasks++; break;
            case "TASK_STARTING": framework.starting_tasks++; break;
            case "TASK_RUNNING": framework.running_tasks++; break;
            case "TASK_KILLING": framework.killing_tasks++; break;
          }
        }
      });

      framework.cpus_share = 0;
      if ($scope.total_cpus > 0) {
        framework.cpus_share = framework.allocated_cpus / $scope.total_cpus;
      }

      framework.gpus_share = 0;
      if ($scope.total_gpus > 0) {
        framework.gpus_share = framework.allocated_gpus / $scope.total_gpus;
      }

      framework.mem_share = 0;
      if ($scope.total_mem > 0) {
        framework.mem_share = framework.allocated_mem / $scope.total_mem;
      }

      framework.disk_share = 0;
      if ($scope.total_disk > 0) {
        framework.disk_share = framework.allocated_disk / $scope.total_disk;
      }

      framework.max_share = Math.max(
        framework.cpus_share,
        framework.gpus_share,
        framework.mem_share,
        framework.disk_share);

      _.each($scope.unreachable_tasks, function (task) {
        if (task.framework_id.value === framework.framework_info.id.value) {
          framework.unreachable_tasks = framework.unreachable_tasks.concat(task);
          switch (task.state) {
            case "TASK_STAGING": framework.staging_tasks++; break;
            case "TASK_STARTING": framework.starting_tasks++; break;
            case "TASK_RUNNING": framework.running_tasks++; break;
            case "TASK_KILLING": framework.killing_tasks++; break;
          }
        }
      });

      _.each($scope.completed_tasks, function (task) {
        if (task.framework_id.value === framework.framework_info.id.value) {
          framework.completed_tasks = framework.completed_tasks.concat(task);
          switch (task.state) {
            case "TASK_FINISHED": framework.finished_tasks++; break;
            case "TASK_KILLED": framework.killed_tasks++; break;
            case "TASK_FAILED": framework.failed_tasks++; break;
            case "TASK_LOST": framework.lost_tasks++; break;
          }
        }
      });

      $scope.frameworks = _.clone($scope.frameworks);
      $scope.frameworks[framework.framework_info.id.value] = framework;
    });
  }

  function frameworkUpdated($scope, framework) {
    return safeApply($scope, function() {
      $scope.frameworks = _.omit($scope.frameworks, framework.framework_info.id.value)
      frameworkAdded(framework);
    });
  }

  function frameworkRemoved($scope, framework_info) {
    return safeApply($scope, function() {
      // TODO(benh): Do we need to be so defensive?
      var framework = _.has($scope.frameworks, framework_info.id.value)
          ? $scope.frameworks[framework_info.id.value]
          : undefined;

      $scope.frameworks = _.omit($scope.frameworks, framework_info.id.value);

      if (!_.isUndefined(framework)) {
        framework.allocated_cpus = 0;
        framework.allocated_gpus = 0;
        framework.allocated_mem = 0;
        framework.allocated_disk = 0;

        // TODO(bmahler): Add per-framework metrics to the master so that
        // the webui does not need to loop over all tasks!
        framework.staging_tasks = 0;
        framework.starting_tasks = 0;
        framework.running_tasks = 0;
        framework.killing_tasks = 0;
        framework.finished_tasks = 0;
        framework.killed_tasks = 0;
        framework.failed_tasks = 0;
        framework.lost_tasks = 0;

        framework.cpus_share = 0;
        framework.gpus_share = 0;
        framework.mem_share = 0;
        framework.disk_share = 0;
        framework.max_share = 0;

        _.each(framework.tasks, function(task) {
          // TODO(benh): Change the task state?
          $scope.active_tasks = _.filter($scope.active_tasks, function(task) {
            return task.framework_id.value !== framework.framework_info.id.value;
          });
          $scope.completed_tasks = $scope.completed_tasks.concat(task);
          framework.completed_tasks = framework.completed_tasks.concat(task);
        });

        _.each(framework.unreachable_tasks, function(task) {
          // TODO(benh): Change the task state?
          $scope.unreachable_tasks = _.filter($scope.unreachable_tasks, function(task) {
            return task.framework_id.value !== framework.framework_info.id.value;
          });
          $scope.completed_tasks = $scope.completed_tasks.concat(task);
          framework.completed_tasks = framework.completed_tasks.concat(task);
        });

        $scope.completed_frameworks = _.clone($scope.completed_frameworks);
        $scope.completed_frameworks[framework.framework_info.id.value] = framework;
      }
    });
  }

  // Update the outermost scope with the metrics/snapshot endpoint.
  function updateMetrics($scope, $timeout, metrics) {
    $scope.staging_tasks = metrics['master/tasks_staging'];
    $scope.starting_tasks = metrics['master/tasks_starting'];
    $scope.running_tasks = metrics['master/tasks_running'];
    $scope.killing_tasks = metrics['master/tasks_killing'];
    $scope.finished_tasks = metrics['master/tasks_finished'];
    $scope.killed_tasks = metrics['master/tasks_killed'];
    $scope.failed_tasks = metrics['master/tasks_failed'];
    $scope.lost_tasks = metrics['master/tasks_lost'];

    return true; // Continue polling.
  }


  // Main controller that can be used to handle "global" events. E.g.,:
  //     $scope.$on('$afterRouteChange', function() { ...; });
  //
  // In addition, the MainCtrl encapsulates the "view", allowing the
  // active controller/view to easily access anything in scope (e.g.,
  // the state).
  mesosApp.controller('MainCtrl', [
      '$scope', '$http', '$location', '$timeout', '$modal',
      function($scope, $http, $location, $timeout, $modal) {
    $scope.doneLoading = true;

    // Adding bindings into scope so that they can be used from within
    // AngularJS expressions.
    $scope._ = _;
    $scope.stringify = JSON.stringify;
    $scope.encodeURIComponent = encodeURIComponent;
    $scope.basename = function(path) {
      // This is only a basic version of basename that handles the cases we care
      // about, rather than duplicating unix basename functionality perfectly.
      if (path === '/') {
        return path;  // Handle '/'.
      }

      // Strip a trailing '/' if present.
      if (path.length > 0 && path.lastIndexOf('/') === (path.length - 1)) {
        path = path.substr(0, path.length - 1);
      }
      return path.substr(path.lastIndexOf('/') + 1);
    };

    $scope.$location = $location;
    $scope.delay = 2000;
    $scope.retry = 0;
    $scope.isErrorModalOpen = false;

    // Pass this pollTime to all relativeDate calls to make them all
    // relative to the same moment in time.
    //
    // If relativeDate is called without a reference time, it
    // instantiates a new Date to be the reference. Since there can be
    // hundreds of dates on a given page, they would all be relative
    // to slightly different moments in time.
    $scope.pollTime = new Date();

    // Ordered Array of path => activeTab mappings. On successful route changes,
    // the `pathRegexp` values are matched against the current route. The first
    // match will be used to set the active navbar tab.
    var NAVBAR_PATHS = [
      {
        pathRegexp: /^\/agents/,
        tab: 'agents'
      },
      {
        pathRegexp: /^\/frameworks/,
        tab: 'frameworks'
      },
      {
        pathRegexp: /^\/roles/,
        tab: 'roles'
      },
      {
        pathRegexp: /^\/offers/,
        tab: 'offers'
      },
      {
        pathRegexp: /^\/maintenance/,
        tab: 'maintenance'
      }
    ];

    // Set the active tab on route changes according to NAVBAR_PATHS.
    $scope.$on('$routeChangeSuccess', function(event, current) {
      var path = current.$$route.originalPath;

      // Use _.some so the loop can exit on the first `pathRegexp` match.
      var matched = _.some(NAVBAR_PATHS, function(nav) {
        if (path.match(nav.pathRegexp)) {
          $scope.navbarActiveTab = nav.tab;
          return true;
        }
      });

      if (!matched) $scope.navbarActiveTab = null;
    });

    var leadingMasterURL = function(path) {
      // Use current location as address in case we could not find the
      // leading master.
      var address = location.hostname + ':' + location.port;
      if ($scope.state && $scope.state.leader) {
          address = $scope.state.leader.hostname + ':' +
                    $scope.state.leader.port;
      }

      return '//' + address + path;
    }

    var popupErrorModal = function() {
      if ($scope.delay >= 128000) {
        $scope.delay = 2000;
      } else {
        $scope.delay = $scope.delay * 2;
      }

      $scope.isErrorModalOpen = true;

      var errorModal = $modal.open({
        controller: function($scope, $modalInstance, scope) {
          // Give the modal reference to the root scope so it can access the
          // `retry` variable. It needs to be passed by reference, not by
          // value, since its value is changed outside the scope of the
          // modal.
          $scope.rootScope = scope;
        },
        resolve: {
          scope: function() { return $scope; }
        },
        templateUrl: "template/dialog/masterGone.html"
      });

      // Make it such that everytime we hide the error-modal, we stop the
      // countdown and restart the polling.
      errorModal.result.then(function() {
        $scope.isErrorModalOpen = false;

        if ($scope.countdown != null) {
          if ($timeout.cancel($scope.countdown)) {
            // Restart since they cancelled the countdown.
            $scope.delay = 2000;
          }
        }

        // Start streaming and polling again, but do it asynchronously
        // (and wait at least a second because otherwise the
        // error-modal won't get properly shown).
        $timeout(streamState, 1000);
        $timeout(pollMetrics, 1000);
      });

      $scope.retry = $scope.delay;
      var countdown = function() {
        if ($scope.retry === 0) {
          errorModal.close();
        } else {
          $scope.retry = $scope.retry - 1000;
          $scope.countdown = $timeout(countdown, 1000);
        }
      };
      countdown();
    };

    var pollMetrics = function() {
      $http.jsonp(leadingMasterURL('/metrics/snapshot?jsonp=JSON_CALLBACK'))
        .success(function(response) {
          if (updateMetrics($scope, $timeout, response)) {
            $scope.delay = updateInterval(_.size($scope.agents));
            $timeout(pollMetrics, $scope.delay);
          }
        })
        .error(function(message, code) {
          if ($scope.isErrorModalOpen === false) {
            // If return code is 401 or 403 the user is unauthorized to reach
            // the endpoint, which is not a connection error.
            if ([401, 403].indexOf(code) < 0) {
              popupErrorModal();
            }
          }
        });
    };

    function getFlags() {
      // TODO(benh): Need to deal with redirected masters so we don't
      // get a CORS error for either `request()` or `stream()` below.

      // Get flags for things like 'cluster' and 'log_dir'.
      request({ type: "GET_FLAGS" })
        .subscribe(
          response => {
            var cluster = _.find(response.get_flags.flags, flag => {
              return flag.name === "cluster";
            });

            if (!_.isUndefined(cluster)) {
              $scope.cluster = cluster.value;
            }

            // Track whether this cluster is named for display purposes.
            $scope.clusterNamed = !!$scope.cluster;

            var external_log_file = _.find(response.get_flags.flags, flag => {
              return flag.name === "external_log_file";
            });

            if (!_.isUndefined(external_log_file)) {
              $scope.external_log_file = external_log_file.value;
            }

            var log_dir = _.find(response.get_flags.flags, flag => {
              return flag.name === "log_dir";
            });

            if (!_.isUndefined(log_dir)) {
              $scope.log_dir = log_dir.value;
            }
          },
          error => console.log(error)
        );
    }

    var streamState = function() {
      // TODO(benh): Need to deal with redirected masters so we don't
      // get a CORS error for either `request()` or `stream()` below.

      // Stream updates from the master.
      stream({ type: "SUBSCRIBE" })
        .subscribe(
          value => {
            var event = JSON.parse(value);
            console.log(event);
            if (event.type === 'SUBSCRIBED') {
              subscribed($scope, event.subscribed.get_state);
            } else if (event.type === 'TASK_ADDED') {
              taskAdded($scope, event.task_added.task);
            } else if (event.type === 'TASK_UPDATED') {
              taskUpdated(
                $scope,
                event.task_updated.framework_id,
                event.task_updated.status,
                event.task_updated.state);
            } else if (event.type === 'AGENT_ADDED') {
              agentAdded($scope, event.agent_added.agent);
            } else if (event.type === 'AGENT_REMOVED') {
              agentRemoved($scope, event.agent_removed.agent_id);
            } else if (event.type === 'FRAMEWORK_ADDED') {
              frameworkAdded($scope, event.framework_added.framework);
            } else if (event.type === 'FRAMEWORK_UPDATED') {
              frameworkUpdated($scope, event.framework_removed.framework);
            } else if (event.type === 'FRAMEWORK_REMOVED') {
              frameworkRemoved($scope, event.framework_removed.framework_info);
            }

            // Broadcast that the state has been updated so any
            // listeners can perform necessary actions to stay up to
            // date (e.g., the roles controller updates it's data by
            // making another request).
            $scope.$broadcast('state_updated');
          },
          error => {
            // TODO(benh): Popup error modal.
            console.log(error);
          },
          () => {
            // TODO(benh): Retry the stream.
            console.log("stream completed")
          }
        );
    };

    getFlags();
    streamState();
    pollMetrics();
  }]);


  mesosApp.controller('HomeCtrl', function($dialog, $scope) {
    $scope.log = function($event) {
      if (!$scope.external_log_file && !$scope.log_dir) {
        $dialog.messageBox(
          'Logging to a file is not enabled',
          "Set the 'external_log_file' or 'log_dir' option if you wish to access the logs.",
          [{label: 'Continue'}]
        ).open();
      } else {
        pailer(
            '//' + $scope.$location.host() + ':' + $scope.$location.port(),
            '/master/log',
            'Mesos Master (' + $scope.$location.host() + ':' + $scope.$location.port() + ')');
      }
    };
  });

  mesosApp.controller('FrameworksCtrl', function() {});

  mesosApp.controller('RolesCtrl', function($scope, $http) {
    var update = function() {
      // TODO(haosdent): Send requests to the leading master directly
      // once `leadingMasterURL` is public.
      $http.jsonp('master/roles?jsonp=JSON_CALLBACK')
      .success(function(response) {
        $scope.roles = response;
      })
      .error(function() {
        if ($scope.isErrorModalOpen === false) {
          popupErrorModal();
        }
      });
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  });

  mesosApp.controller('OffersCtrl', function() {});

  mesosApp.controller('MaintenanceCtrl', function($scope, $http) {
    var update = function() {
      // TODO(haosdent): Send requests to the leading master directly
      // once `leadingMasterURL` is public.
      $http.jsonp('master/maintenance/schedule?jsonp=JSON_CALLBACK')
      .success(function(response) {
        $scope.maintenance = response;
      })
      .error(function() {
        if ($scope.isErrorModalOpen === false) {
          popupErrorModal();
        }
      });
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  });

  mesosApp.controller('FrameworkCtrl', function($scope, $routeParams) {
    var update = function() {
      if ($routeParams.id in $scope.completed_frameworks) {
        $scope.framework = $scope.completed_frameworks[$routeParams.id];
        $scope.alert_message = 'This framework has terminated!';
        $('#alert').show();
        $('#framework').show();
      } else if ($routeParams.id in $scope.frameworks) {
        $scope.framework = $scope.frameworks[$routeParams.id];
        $('#framework').show();
      } else {
        $scope.alert_message = 'No framework found with ID: ' + $routeParams.id;
        $('#alert').show();
      }
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  });


  mesosApp.controller('AgentsCtrl', function() {});


  mesosApp.controller('AgentCtrl', [
      '$dialog', '$scope', '$routeParams', '$http', '$q', '$timeout', 'top',
      function($dialog, $scope, $routeParams, $http, $q, $timeout, $top) {
    $scope.agent_id = $routeParams.agent_id;

    var update = function() {
      if (!($routeParams.agent_id in $scope.agents)) {
        $scope.alert_message = 'No agent found with ID: ' + $routeParams.agent_id;
        $('#alert').show();
        return;
      }

      var agent = $scope.agents[$routeParams.agent_id];

      $scope.log = function($event) {
        if (!$scope.state.external_log_file && !$scope.state.log_dir) {
          $dialog.messageBox(
            'Logging to a file is not enabled',
            "Set the 'external_log_file' or 'log_dir' option if you wish to access the logs.",
            [{label: 'Continue'}]
          ).open();
        } else {
          pailer(agentURLPrefix(agent, false), '/slave/log', 'Mesos Agent (' + agent.agent_info.id + ')');
        }
      };


      // Set up polling for the monitor if this is the first update.
      if (!$top.started()) {
        $top.start(
          agentURLPrefix(agent, true) + '/monitor/statistics?jsonp=JSON_CALLBACK',
          $scope
        );
      }

      $http.jsonp(agentURLPrefix(agent, true) + '/state?jsonp=JSON_CALLBACK')
        .success(function (response) {
          $scope.state = response;

          $scope.agent = {};
          $scope.agent.frameworks = {};
          $scope.agent.completed_frameworks = {};

          // Convert the reserved resources map into an array for inclusion
          // in an `ng-repeat` table.
          $scope.agent.reserved_resources_as_array = _($scope.state.reserved_resources)
            .map(function(reservation, role) {
              reservation.role = role;
              return reservation;
            });

          // Computes framework stats by setting new attributes on the 'framework'
          // object.
          function computeFrameworkStats(framework) {
            framework.num_tasks = 0;
            framework.cpus = 0;
            framework.gpus = 0;
            framework.mem = 0;
            framework.disk = 0;

            _.each(framework.executors, function(executor) {
              framework.num_tasks += _.size(executor.tasks);
              framework.cpus += executor.resources.cpus;
              framework.gpus += executor.resources.gpus;
              framework.mem += executor.resources.mem;
              framework.disk += executor.resources.disk;
            });
          }

          // Compute framework stats and update agent's mappings of those
          // frameworks.
          _.each($scope.state.frameworks, function(framework) {
            $scope.agent.frameworks[framework.id] = framework;
            computeFrameworkStats(framework);

            // Fill in the `roles` field for non-MULTI_ROLE schedulers.
            if (framework.role) {
              framework.roles = [framework.role];
            }
          });

          _.each($scope.state.completed_frameworks, function(framework) {
            $scope.agent.completed_frameworks[framework.id] = framework;
            computeFrameworkStats(framework);

            // Fill in the `roles` field for non-MULTI_ROLE schedulers.
            if (framework.role) {
              framework.roles = [framework.role];
            }
          });

          $scope.state.allocated_resources = {};
          $scope.state.allocated_resources.cpus = 0;
          $scope.state.allocated_resources.gpus = 0;
          $scope.state.allocated_resources.mem = 0;
          $scope.state.allocated_resources.disk = 0;

          // Currently the agent does not expose the total allocated
          // resources across all frameworks, so we sum manually.
          _.each($scope.state.frameworks, function(framework) {
            $scope.state.allocated_resources.cpus += framework.cpus;
            $scope.state.allocated_resources.gpus += framework.gpus;
            $scope.state.allocated_resources.mem += framework.mem;
            $scope.state.allocated_resources.disk += framework.disk;
          });

          $('#agent').show();
        })
        .error(function(reason) {
          $scope.alert_message = 'Failed to get agent usage / state: ' + reason;
          $('#alert').show();
        });

      $http.jsonp(agentURLPrefix(agent, false) + '/metrics/snapshot?jsonp=JSON_CALLBACK')
        .success(function (response) {
          $scope.staging_tasks = response['slave/tasks_staging'];
          $scope.starting_tasks = response['slave/tasks_starting'];
          $scope.running_tasks = response['slave/tasks_running'];
          $scope.killing_tasks = response['slave/tasks_killing'];
          $scope.finished_tasks = response['slave/tasks_finished'];
          $scope.killed_tasks = response['slave/tasks_killed'];
          $scope.failed_tasks = response['slave/tasks_failed'];
          $scope.lost_tasks = response['slave/tasks_lost'];
        })
        .error(function(reason) {
          $scope.alert_message = 'Failed to get agent metrics: ' + reason;
          $('#alert').show();
        });
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  }]);


  mesosApp.controller('AgentFrameworkCtrl', [
      '$scope', '$routeParams', '$http', '$q', '$timeout', 'top',
      function($scope, $routeParams, $http, $q, $timeout, $top) {
    $scope.agent_id = $routeParams.agent_id;
    $scope.framework_id = $routeParams.framework_id;

    var update = function() {
      if (!($routeParams.agent_id in $scope.agents)) {
        $scope.alert_message = 'No agent found with ID: ' + $routeParams.agent_id;
        $('#alert').show();
        return;
      }

      var agent = $scope.agents[$routeParams.agent_id];

      // Set up polling for the monitor if this is the first update.
      if (!$top.started()) {
        $top.start(
          agentURLPrefix(agent, true) + '/monitor/statistics?jsonp=JSON_CALLBACK',
          $scope
        );
      }

      $http.jsonp(agentURLPrefix(agent, true) + '/state?jsonp=JSON_CALLBACK')
        .success(function (response) {
          $scope.state = response;

          $scope.agent = {};

          function matchFramework(framework) {
            return $scope.framework_id === framework.id;
          }

          // Find the framework; it's either active or completed.
          $scope.framework =
              _.find($scope.state.frameworks, matchFramework) ||
              _.find($scope.state.completed_frameworks, matchFramework);

          if (!$scope.framework) {
            $scope.alert_message = 'No framework found with ID: ' + $routeParams.framework_id;
            $('#alert').show();
            return;
          }

          // Fill in the `roles` field for non-MULTI_ROLE schedulers.
          if ($scope.framework.role) {
            $scope.framework.roles = [$scope.framework.role];
          }

          // Compute the framework stats.
          $scope.framework.num_tasks = 0;
          $scope.framework.cpus = 0;
          $scope.framework.gpus = 0;
          $scope.framework.mem = 0;
          $scope.framework.disk = 0;

          _.each($scope.framework.executors, function(executor) {
            $scope.framework.num_tasks += _.size(executor.tasks);
            $scope.framework.cpus += executor.resources.cpus;
            $scope.framework.gpus += executor.resources.gpus;
            $scope.framework.mem += executor.resources.mem;
            $scope.framework.disk += executor.resources.disk;

            // If 'role' is not present in executor, we are talking
            // to a non-MULTI_ROLE capable agent. This means that we
            // can use the 'role' of the framework.
            executor.role = executor.role || $scope.framework.role;
          });

          $('#agent').show();
        })
        .error(function (reason) {
          $scope.alert_message = 'Failed to get agent usage / state: ' + reason;
          $('#alert').show();
        });
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  }]);


  mesosApp.controller('AgentExecutorCtrl', [
      '$scope', '$routeParams', '$http', '$q', '$timeout', 'top',
      function($scope, $routeParams, $http, $q, $timeout, $top) {
    $scope.agent_id = $routeParams.agent_id;
    $scope.framework_id = $routeParams.framework_id;
    $scope.executor_id = $routeParams.executor_id;

    var update = function() {
      if (!($routeParams.agent_id in $scope.agents)) {
        $scope.alert_message = 'No agent found with ID: ' + $routeParams.agent_id;
        $('#alert').show();
        return;
      }

      var agent = $scope.agents[$routeParams.agent_id];

      // Set up polling for the monitor if this is the first update.
      if (!$top.started()) {
        $top.start(
          agentURLPrefix(agent, true) + '/monitor/statistics?jsonp=JSON_CALLBACK',
          $scope
        );
      }

      $http.jsonp(agentURLPrefix(agent, true) + '/state?jsonp=JSON_CALLBACK')
        .success(function (response) {
          $scope.state = response;

          $scope.agent = {};

          function matchFramework(framework) {
            return $scope.framework_id === framework.id;
          }

          // Find the framework; it's either active or completed.
          $scope.framework =
            _.find($scope.state.frameworks, matchFramework) ||
            _.find($scope.state.completed_frameworks, matchFramework);

          if (!$scope.framework) {
            $scope.alert_message = 'No framework found with ID: ' + $routeParams.framework_id;
            $('#alert').show();
            return;
          }

          function matchExecutor(executor) {
            return $scope.executor_id === executor.id;
          }

          function setRole(tasks) {
            _.each(tasks, function(task) {
              task.role = $scope.framework.role;
            });
          }

          function setHealth(tasks) {
            _.each(tasks, function(task) {
              var lastStatus = _.last(task.statuses);
              if (lastStatus) {
                task.healthy = lastStatus.healthy;
              }
            })
          };

          // Look for the executor; it's either active or completed.
          $scope.executor =
            _.find($scope.framework.executors, matchExecutor) ||
            _.find($scope.framework.completed_executors, matchExecutor);

          if (!$scope.executor) {
            $scope.alert_message = 'No executor found with ID: ' + $routeParams.executor_id;
            $('#alert').show();
            return;
          }

          // If 'role' is not present in the task, we are talking
          // to a non-MULTI_ROLE capable agent. This means that we
          // can use the 'role' of the framework.
          if (!("role" in $scope.executor)) {
            $scope.executor.role = $scope.framework.role;

            setRole($scope.executor.tasks);
            setRole($scope.executor.queued_tasks);
            setRole($scope.executor.completed_tasks);
          }

          setHealth($scope.executor.tasks);
          setTaskSandbox($scope.executor);

          $('#agent').show();
        })
        .error(function (reason) {
          $scope.alert_message = 'Failed to get agent usage / state: ' + reason;
          $('#alert').show();
        });
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  }]);


  // Reroutes requests like:
  //   * '/agents/:agent_id/frameworks/:framework_id/executors/:executor_id/browse'
  //   * '/agents/:agent_id/frameworks/:framework_id/executors/:executor_id/tasks/:task_id/browse'
  // to the sandbox directory of the executor or the task respectively. This
  // requires a second request because the directory to browse is known by the
  // agent but not by the master. Request the directory from the agent, and then
  // redirect to it.
  //
  // TODO(ssorallen): Add `executor.directory` to the master's state endpoint
  // output so this controller of rerouting is no longer necessary.
  mesosApp.controller('AgentTaskAndExecutorRerouterCtrl',
      function($alert, $http, $location, $routeParams, $scope, $window) {

    function goBack(flashMessageOrOptions) {
      if (flashMessageOrOptions) {
        $alert.danger(flashMessageOrOptions);
      }

      if ($window.history.length > 1) {
        // If the browser has something in its history, just go back.
        $window.history.back();
      } else {
        // Otherwise navigate to the framework page, which is likely the
        // previous page anyway.
        $location.path('/frameworks/' + $routeParams.framework_id).replace();
      }
    }

    var reroute = function() {
      var agent = $scope.agents[$routeParams.agent_id];

      // If the agent doesn't exist, send the user back.
      if (!agent) {
        return goBack("Agent with ID '" + $routeParams.agent_id + "' does not exist.");
      }

      // Request agent details to get access to the route executor's "directory"
      // to navigate directly to the executor's sandbox.
      var url = agentURLPrefix(agent, true) + '/state?jsonp=JSON_CALLBACK';
      $http.jsonp(url)
        .success(function(response) {

          function matchFramework(framework) {
            return $routeParams.framework_id === framework.id;
          }

          var framework =
            _.find(response.frameworks, matchFramework) ||
            _.find(response.completed_frameworks, matchFramework);

          if (!framework) {
            return goBack(
              "Framework with ID '" + $routeParams.framework_id +
                "' does not exist on agent with ID '" + $routeParams.agent_id +
                "'."
            );
          }

          function matchExecutor(executor) {
            return $routeParams.executor_id === executor.id;
          }

          var executor =
            _.find(framework.executors, matchExecutor) ||
            _.find(framework.completed_executors, matchExecutor);

          if (!executor) {
            return goBack(
              "Executor with ID '" + $routeParams.executor_id +
                "' does not exist on agent with ID '" + $routeParams.agent_id +
                "'."
            );
          }

          var sandboxDirectory = executor.directory;

          function matchTask(task) {
            return $routeParams.task_id === task.id;
          }

          // Continue to navigate to the task's sandbox if the task id is
          // specified in route parameters.
          if ($routeParams.task_id) {
            setTaskSandbox(executor);

            var task =
              _.find(executor.tasks, matchTask) ||
              _.find(executor.queued_tasks, matchTask) ||
              _.find(executor.completed_tasks, matchTask);

            if (!task) {
              return goBack(
                "Task with ID '" + $routeParams.task_id +
                  "' does not exist on agent with ID '" + $routeParams.agent_id +
                  "'."
              );
            }

            sandboxDirectory = task.directory;
          }

          // Navigate to a path like '/agents/:id/browse?path=%2Ftmp%2F', the
          // recognized "browse" endpoint for an agent.
          $location.path('/agents/' + $routeParams.agent_id + '/browse')
            .search({path: sandboxDirectory})
            .replace();
        })
        .error(function(response) {
          $alert.danger({
            bullets: [
             "The agent is not accessible",
             "The agent timed out or went offline"
            ],
            message: "Potential reasons:",
            title: "Failed to connect to agent '" + $routeParams.agent_id +
              "' on '" + url + "'."
          });

          // Is the agent dead? Navigate home since returning to the agent might
          // end up in an endless loop.
          $location.path('/').replace();
        });
    };

    // When navigating directly to this page, e.g. pasting the URL into the
    // browser, the previous page is not a page in Mesos. The agents
    // information may not ready when loading this page, we start to reroute
    // the sandbox request after the agents information loaded.
    if ($scope.state) {
      reroute();
    }

    // `reroute` is expected to always route away from the current page
    // and the listener would be removed after the first state update.
    var removeListener = $scope.$on('state_updated', reroute);
    $scope.$on('$routeChangeStart', removeListener);
  });


  mesosApp.controller('BrowseCtrl', function($scope, $routeParams, $http) {
    var update = function() {
      if ($routeParams.agent_id in $scope.agents && $routeParams.path) {
        $scope.agent_id = $routeParams.agent_id;
        $scope.path = $routeParams.path;

        var agent = $scope.agents[$routeParams.agent_id];

        // This variable is used in 'browse.html' to generate the '/files'
        // links, so we have to pass `includeProcessId=false` (see
        // `agentURLPrefix`for more details).
        $scope.agent_url_prefix = agentURLPrefix(agent, false);

        $scope.pail = function($event, path) {
          pailer(
            agentURLPrefix(agent, false),
            path,
            decodeURIComponent(path) + ' (' + agent.agent_info.id + ')');
        };

        var url = agentURLPrefix(agent, false) + '/files/browse?jsonp=JSON_CALLBACK';

        // TODO(bmahler): Try to get the error code / body in the error callback.
        // This wasn't working with the current version of angular.
        $http.jsonp(url, {params: {path: $routeParams.path}})
          .success(function(data) {
            $scope.listing = data;
            $('#listing').show();
          })
          .error(function() {
            $scope.alert_message = 'Error browsing path: ' + $routeParams.path;
            $('#alert').show();
          });
      } else {
        if (!($routeParams.agent_id in $scope.agents)) {
          $scope.alert_message = 'No agent found with ID: ' + $routeParams.agent_id;
        } else {
          $scope.alert_message = 'Missing "path" request parameter.';
        }
        $('#alert').show();
      }
    };

    if ($scope.state) {
      update();
    }

    var removeListener = $scope.$on('state_updated', update);
    $scope.$on('$routeChangeStart', removeListener);
  });
})();
