#ifndef __COMMON_WEBUI_UTILS_HPP__
#define __COMMON_WEBUI_UTILS_HPP__

#include <string>

#include "configurator/configuration.hpp"

namespace mesos {
namespace internal {
namespace utils {
namespace webui {

#ifdef MESOS_WEBUI
void spawnWebui(const Configuration& conf,
                const std::string& script,
                int rpcPort, int defaultWebuiPort);
#endif

} // namespace process {
} // namespace utils {
} // namespace internal {
} // namespace mesos {

#endif
