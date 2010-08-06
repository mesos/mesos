#ifndef __FAIR_ALLOCATOR_HPP__
#define __FAIR_ALLOCATOR_HPP__

#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "allocator.hpp"
#include "resources.hpp"

namespace mesos { namespace internal { namespace fair_allocator {

using std::string;
using std::vector;
using boost::unordered_map;
using boost::unordered_set;
using namespace mesos::internal::master;

struct UserInfo;

/**
 * An allocation module that performs fair sharing between users using
 * Dominant Resource Fairness (DRF). Also supports task killing using a
 * configurable SLO for each user (a number of tasks of a particular size).
 */
class FairAllocator : public Allocator {
  Master* master;

  // Total resources available in the Mesos cluster
  Resources totalResources;
  
  // Remember which frameworks refused each slave "recently"; this is cleared
  // when the slave's free resources go up or when everyone has refused it
  unordered_map<Slave*, unordered_set<Framework*> > refusers;

  unordered_map<string, UserInfo*> userInfos;
  
public:
  FairAllocator(Master* _master): master(_master) {}
  
  ~FairAllocator();
  
  virtual void frameworkAdded(Framework* framework);
  
  virtual void frameworkRemoved(Framework* framework);
  
  virtual void slaveAdded(Slave* slave);
  
  virtual void slaveRemoved(Slave* slave);
  
  virtual void taskRemoved(Task* task, TaskRemovalReason reason);

  virtual void offerReturned(SlotOffer* offer,
                             OfferReturnReason reason,
                             const vector<SlaveResources>& resourcesLeft);

  virtual void offersRevived(Framework* framework);
  
  virtual void timerTick();
  
private:
  // Get or create the UserInfo object for a given user
  UserInfo* getUserInfo(const string& user);

  // Get or create the UserInfo object for the user who owns a given framework
  UserInfo* getUserInfo(Framework* framework);

  // Get an ordering to consider frameworks in for launching tasks
  vector<Framework*> getAllocationOrdering();
  
  // Look at the full state of the cluster and send out offers
  void makeNewOffers();

  // Make resource offers for just one slave
  void makeNewOffers(Slave* slave);

  // Make resource offers for a subset of the slaves
  void makeNewOffers(const vector<Slave*>& slaves);
};


/**
 * Information kept by the FairAllocator for each user.
 */
struct UserInfo
{
  string name;
  double weight;       // Weight used in fair sharing
  unordered_set<Framework*> frameworks; // Active frameworks owned by user
  Resources resources; // Total resources owned by this user; this is
                       // updated only when updateResources() is called
  
  UserInfo(const string& _name): name(_name), weight(1.0) {}

  // Update the resources field by re-counting all our frameworks' resources
  void updateResources();
};


}}} /* namespace */

#endif /* __FAIR_ALLOCATOR_HPP__ */
