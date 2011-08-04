#ifndef __SLAVE_WEBUI_HPP__
#define __SLAVE_WEBUI_HPP__

#include <process/process.hpp>

#include "slave.hpp"

#include "config/config.hpp"


#ifdef MESOS_WEBUI

namespace mesos {
namespace internal {
namespace slave {
namespace webui {

void start(const process::PID<Slave>& slave, const Configuration& conf);

} // namespace webui {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // MESOS_WEBUI
#endif // __SLAVE_WEBUI_HPP__
