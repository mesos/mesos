// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

#ifndef __PROCESS_ROUTE_HPP__
#define __PROCESS_ROUTE_HPP__

#include <process/http.hpp>
#include <process/process.hpp>

#include <stout/strings.hpp>

namespace process {
namespace http {

// Helper for creating routes without a process.
class Route
{
public:
  // TODO(benh): Support `Process::RouteOptions` or equivalant.
  Route(const std::string& name,
        const Option<std::string>& help,
        const lambda::function<Future<Response>(const Request&)>& handler)
    : process(name, help, handler)
  {
    spawn(process);
  }

  ~Route()
  {
    terminate(process);
    wait(process);
  }

private:
  class RouteProcess : public Process<RouteProcess>
  {
  public:
    RouteProcess(
        const std::string& name,
        const Option<std::string>& _help,
        const lambda::function<Future<Response>(const Request&)>& _handler)
      : ProcessBase(strings::remove(name, "/", strings::PREFIX)),
        help(_help),
        handler(_handler) {}

  protected:
    void initialize() override
    {
      route("/", help, handler);
    }

    const Option<std::string> help;
    const lambda::function<Future<Response>(const Request&)> handler;
  };

  RouteProcess process;
};

} // namespace http {
} // namespace process {

#endif // __PROCESS_ROUTE_HPP__
