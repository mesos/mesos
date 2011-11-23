#ifndef __PROCESS_UTILS_HPP__
#define __PROCESS_UTILS_HPP__

#include <iostream>
#include <sstream>

#include "common/utils.hpp"


namespace mesos {
namespace internal {
namespace utils {
namespace process {

inline Try<int> killtree(
    pid_t pid,
    int signal,
    bool killgroups,
    bool killsess)
{
  std::string cmdline;
  // TODO(Charles Reiss): Use a configuration option.
  if (utils::os::hasenv("MESOS_KILLTREE")) {
    // Set by mesos-build-env.sh.
    cmdline = utils::os::getenv("MESOS_KILLTREE");
  } else if (utils::os::hasenv("MESOS_SOURCE_DIR")) {
    // Set by test hardness for external tests.
    cmdline = utils::os::getenv("MESOS_SOURCE_DIR") +
        "/src/scripts/killtree.sh";
  } else {
    cmdline = MESOS_LIBEXECDIR "/killtree.sh";
  }
  cmdline += " -p " + pid;
  cmdline += " -s " + signal;
  if (killgroups) cmdline += " -g";
  if (killsess) cmdline += " -x";

  return utils::os::shell(NULL, cmdline);
}

} // namespace mesos {
} // namespace internal {
} // namespace utils {
} // namespace process {

#endif // __PROCESS_UTILS_HPP__
