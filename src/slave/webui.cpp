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

#include <process/process.hpp>

#include "slave/webui.hpp"

#include "common/webui_utils.hpp"

#include "configurator/configuration.hpp"

#ifdef MESOS_WEBUI

namespace mesos {
namespace internal {
namespace slave {
namespace webui {

void start(const process::PID<Slave>& slave, const Configuration& conf)
{
  utils::webui::start(conf, "webui/slave/webui.py", slave.port, 8081);
}

} // namespace webui {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // MESOS_WEBUI
