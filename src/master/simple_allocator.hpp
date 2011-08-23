#ifndef __SIMPLE_ALLOCATOR_HPP__
#define __SIMPLE_ALLOCATOR_HPP__

#include <vector>

#include "common/hashmap.hpp"
#include "common/multihashmap.hpp"

#include "master/allocator.hpp"


namespace mesos {
namespace internal {
namespace master {

class SimpleAllocator : public Allocator
{
public:
  SimpleAllocator(): initialized(false) {}

  virtual ~SimpleAllocator() {}

  virtual void initialize(Master* _master);

  virtual void frameworkAdded(Framework* framework);

  virtual void frameworkRemoved(Framework* framework);

  virtual void slaveAdded(Slave* slave);

  virtual void slaveRemoved(Slave* slave);

  virtual void resourcesRequested(
      const FrameworkID& frameworkId,
      const std::vector<ResourceRequest>& requests);

  virtual void resourcesUnused(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& resources);

  virtual void resourcesRecovered(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& resources);

  virtual void offersRevived(Framework* framework);

  virtual void timerTick();

private:
  // Get an ordering to consider frameworks in for launching tasks.
  std::vector<Framework*> getAllocationOrdering();

  // Look at the full state of the cluster and send out offers.
  void makeNewOffers();

  // Make resource offers for just one slave.
  void makeNewOffers(Slave* slave);

  // Make resource offers for a subset of the slaves.
  void makeNewOffers(const std::vector<Slave*>& slaves);

  bool initialized;

  Master* master;

  Resources totalResources;

  // Remember which frameworks refused each slave "recently"; this is
  // cleared when the slave's free resources go up or when everyone
  // has refused it.
  multihashmap<SlaveID, FrameworkID> refusers;
};

} // namespace master {
} // namespace internal {
} // namespace mesos {

#endif // __SIMPLE_ALLOCATOR_HPP__
