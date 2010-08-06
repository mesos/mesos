#include "allocator_factory.hpp"
#include "simple_allocator.hpp"
#include "fair_allocator.hpp"

using namespace mesos::internal::master;
using mesos::internal::simple_allocator::SimpleAllocator;
using mesos::internal::fair_allocator::FairAllocator;

DEFINE_FACTORY(Allocator, Master *)
{
  registerClass<SimpleAllocator>("simple");
  registerClass<FairAllocator>("fair");
}
