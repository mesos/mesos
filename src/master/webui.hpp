#ifndef __MASTER_WEBUI_HPP__
#define __MASTER_WEBUI_HPP__

#include <process/process.hpp>

#include "master.hpp"

#include "config/config.hpp"


#ifdef MESOS_WEBUI

namespace mesos {
namespace internal {
namespace master {
namespace webui {

void start(const process::PID<Master>& master, const Configuration& conf);

} // namespace webui {
} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // MESOS_WEBUI
#endif // __MASTER_WEBUI_HPP__ 
