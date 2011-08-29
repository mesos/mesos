#ifndef __PROCESS_UTILS_HPP__
#define __PROCESS_UTILS_HPP__

#include "common/utils.hpp"

#include <iostream>
#include <sstream>


namespace mesos {
namespace internal {
namespace utils {
namespace process {

inline Result<int> killtree(pid_t pid,
                            int signal,
                            bool killgroups,
                            bool killsess)
{
    CHECK(utils::os::hasenv("MESOS_HOME")) << " MESOS_HOME is not set";

    std::string killcmd = getenv("MESOS_HOME");
    killcmd += "/killtree.sh";
    killcmd += " -p " + pid;
    killcmd += " -s " + signal;
    if (killgroups) killcmd += " -g";
    if (killsess) killcmd += " -x";

    return utils::os::shell(NULL, killcmd);
}
} // namespace mesos {
} // namespace internal {
} // namespace utils {
} // namespace process {

#endif // __PROCESS_UTILS_HPP__
