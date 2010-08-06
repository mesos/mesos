#include <algorithm>

#include <glog/logging.h>

#include "fair_allocator.hpp"


using std::max;
using std::sort;
using std::string;
using std::vector;

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::master;
using namespace mesos::internal::fair_allocator;


FairAllocator::~FairAllocator()
{
  // Delete all UserInfos
  foreachpair (const string& user, UserInfo* info, userInfos) {
    delete info;
  }
  userInfos.clear();
}


UserInfo* FairAllocator::getUserInfo(const string& user)
{
  unordered_map<string, UserInfo*>::iterator it = userInfos.find(user);
  if (it != userInfos.end()) {
    return it->second;
  } else {
    // Create a new UserInfo for this user
    UserInfo* info = new UserInfo(user);
    userInfos[user] = info;
    return info;
  }
}


UserInfo* FairAllocator::getUserInfo(Framework* framework)
{
  return getUserInfo(framework->user);
}


void FairAllocator::frameworkAdded(Framework* framework)
{
  LOG(INFO) << "Added " << framework;
  getUserInfo(framework)->frameworks.insert(framework);
  makeNewOffers();
}


void FairAllocator::frameworkRemoved(Framework* framework)
{
  LOG(INFO) << "Removed " << framework;
  foreachpair (Slave* s, unordered_set<Framework*>& refs, refusers) {
    refs.erase(framework);
  }
  getUserInfo(framework)->frameworks.erase(framework);
  // TODO: Re-offer just the slaves that the framework had tasks on?
  //       Alternatively, comment this out and wait for a timer tick
  makeNewOffers();
}


void FairAllocator::slaveAdded(Slave* slave)
{
  LOG(INFO) << "Added " << slave;
  refusers[slave] = unordered_set<Framework*>();
  totalResources += slave->resources;
  makeNewOffers(slave);
}


void FairAllocator::slaveRemoved(Slave* slave)
{
  LOG(INFO) << "Removed " << slave;
  totalResources -= slave->resources;
  refusers.erase(slave);
}


void FairAllocator::taskRemoved(Task* task, TaskRemovalReason reason)
{
  LOG(INFO) << "Removed " << task;
  // Remove all refusers from this slave since it has more resources free
  Slave* slave = master->lookupSlave(task->slaveId);
  CHECK(slave != 0);
  refusers[slave].clear();
  // Re-offer the resources, unless this task was removed due to a lost
  // slave or a lost framework (in which case we'll get another callback)
  if (reason == TRR_TASK_ENDED || reason == TRR_EXECUTOR_LOST)
    makeNewOffers(slave);
}


void FairAllocator::offerReturned(SlotOffer* offer,
                                    OfferReturnReason reason,
                                    const vector<SlaveResources>& resLeft)
{
  LOG(INFO) << "Offer returned: " << offer << ", reason = " << reason;
  // If this offer returned due to the framework replying, add it to refusers
  if (reason == ORR_FRAMEWORK_REPLIED) {
    Framework* framework = master->lookupFramework(offer->frameworkId);
    CHECK(framework != 0);
    foreach (const SlaveResources& r, resLeft) {
      VLOG(1) << "Framework reply leaves " << r.resources 
              << " free on " << r.slave;
      if (r.resources.cpus > 0 || r.resources.mem > 0) {
        VLOG(1) << "Inserting " << framework << " as refuser for " << r.slave;
        refusers[r.slave].insert(framework);
      }
    }
  }
  // Make new offers, unless the offer returned due to a lost framework or slave
  // (in those cases, frameworkRemoved and slaveRemoved will be called later)
  if (reason != ORR_SLAVE_LOST && reason != ORR_FRAMEWORK_LOST) {
    vector<Slave*> slaves;
    foreach (const SlaveResources& r, resLeft)
      slaves.push_back(r.slave);
    makeNewOffers(slaves);
  }
}


void FairAllocator::offersRevived(Framework* framework)
{
  LOG(INFO) << "Filters removed for " << framework;
  makeNewOffers();
}


void FairAllocator::timerTick()
{
  // TODO: Is this necessary?
  makeNewOffers();
}


namespace {

// DRF comparator functions for Frameworks

Resources getFrameworkResources(Framework* fw) {
  return fw->resources;
}


string getFrameworkId(Framework* fw) {
  return fw->id;
}


double getFrameworkWeight(Framework* fw) {
  return 1.0;
}


// DRF comparator functions for UserInfos

Resources getUserResources(UserInfo* u) {
  return u->resources;
}


string getUserId(UserInfo* u) {
  return u->name;
}


double getUserWeight(UserInfo* u) {
  return u->weight;
}

} /* namespace */


vector<Framework*> FairAllocator::getAllocationOrdering()
{
  // First update each user's resource count and sort the users by DRF
  vector<UserInfo*> users;
  foreachpair (const string& name, UserInfo* info, userInfos) {
    info->updateResources();
    users.push_back(info);
  }
  DrfComparator<UserInfo*> userComp(totalResources,
                                    getUserResources,
                                    getUserWeight,
                                    getUserId);
  sort(users.begin(), users.end(), userComp);

  // Now sort each user's frameworks by DRF and append them to an ordering
  vector<Framework*> ordering;
  DrfComparator<Framework*> frameworkComp(totalResources,
                                          getFrameworkResources,
                                          getFrameworkWeight,
                                          getFrameworkId);
  foreach (UserInfo* info, users) {
    vector<Framework*> userFrameworks(info->frameworks.begin(),
                                      info->frameworks.end());
    sort(userFrameworks.begin(), userFrameworks.end(), frameworkComp);
    foreach (Framework* fw, userFrameworks) {
      ordering.push_back(fw);
    }
  }
  return ordering;
}


void FairAllocator::makeNewOffers()
{
  // TODO: Create a method in master so that we don't return the whole
  // list of slaves
  vector<Slave*> slaves = master->getActiveSlaves();
  makeNewOffers(slaves);
}


void FairAllocator::makeNewOffers(Slave* slave)
{
  vector<Slave*> slaves;
  slaves.push_back(slave);
  makeNewOffers(slaves);
}


void FairAllocator::makeNewOffers(const vector<Slave*>& slaves)
{
  // Get an ordering of frameworks to send offers to
  vector<Framework*> ordering = getAllocationOrdering();
  if (ordering.size() == 0)
    return;
  
  // Find all the free resources that can be allocated
  unordered_map<Slave* , Resources> freeResources;
  foreach (Slave* slave, slaves) {
    if (slave->active) {
      Resources res = slave->resourcesFree();
      if (res.cpus >= MIN_CPUS && res.mem >= MIN_MEM) {
        VLOG(1) << "Found free resources: " << res << " on " << slave;
        freeResources[slave] = res;
      }
    }
  }
  if (freeResources.size() == 0)
    return;
  
  // Clear refusers on any slave that has been refused by everyone
  foreachpair (Slave* slave, _, freeResources) {
    unordered_set<Framework*>& refs = refusers[slave];
    if (refs.size() == ordering.size()) {
      VLOG(1) << "Clearing refusers for " << slave
              << " because everyone refused it";
      refs.clear();
    }
  }
  
  foreach (Framework* framework, ordering) {
    // See which resources this framework can take (given filters & refusals)
    vector<SlaveResources> offerable;
    foreachpair (Slave* slave, Resources resources, freeResources) {
      if (refusers[slave].find(framework) == refusers[slave].end() &&
          !framework->filters(slave, resources)) {
        VLOG(1) << "Offering " << resources << " on " << slave
                << " to framework " << framework->id;
        offerable.push_back(SlaveResources(slave, resources));
      }
    }
    if (offerable.size() > 0) {
      foreach (SlaveResources& r, offerable) {
        freeResources.erase(r.slave);
      }
      master->makeOffer(framework, offerable);
    }
  }
}


void FairAllocator::reloadConfig()
{
}


void FairAllocator::reloadConfigIfNecessary()
{
}


void UserInfo::updateResources()
{
  Resources res;
  foreach (Framework* fw, frameworks) {
    res += fw->resources;
  }
  this->resources = res;
}
