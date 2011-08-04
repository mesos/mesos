#ifndef __MASTER_CONSTANTS_HPP__
#define __MASTER_CONSTANTS_HPP__

namespace mesos {
namespace internal {
namespace master {

// TODO(benh): Add units after constants.
// TODO(benh): Also make configuration options be constants.

// Maximum number of slot offers to have outstanding for each framework.
const int MAX_OFFERS_PER_FRAMEWORK = 50;

// Default number of seconds until a refused slot is resent to a framework.
const double DEFAULT_REFUSAL_TIMEOUT = 5;

// Minimum number of cpus / task.
const int32_t MIN_CPUS = 1;

// Minimum amount of memory / task.
const int32_t MIN_MEM = 32 * Megabyte;

// Maximum number of CPUs per machine.
const int32_t MAX_CPUS = 1000 * 1000;

// Maximum amount of memory / machine.
const int32_t MAX_MEM = 1024 * 1024 * Megabyte;

// Acceptable timeout for slave PONG.
const double SLAVE_PONG_TIMEOUT = 15.0;

// Maximum number of timeouts until slave is considered failed.
const int MAX_SLAVE_TIMEOUTS = 5;

// Time to wait for a framework to failover (TODO(benh): Make configurable)).
const double FRAMEWORK_FAILOVER_TIMEOUT = 60 * 60 * 24;

// Reasons why offers might be returned to the Allocator.
enum OfferReturnReason
{
  ORR_FRAMEWORK_REPLIED,
  ORR_OFFER_RESCINDED,
  ORR_FRAMEWORK_LOST,
  ORR_FRAMEWORK_FAILOVER,
  ORR_SLAVE_LOST
};


// Reasons why tasks might be removed, passed to the Allocator.
enum TaskRemovalReason
{
  TRR_TASK_ENDED,
  TRR_FRAMEWORK_LOST,
  TRR_EXECUTOR_LOST,
  TRR_SLAVE_LOST
};

} // namespace mesos {
} // namespace internal {
} // namespace master {

#endif // __MASTER_CONSTANTS_HPP__
