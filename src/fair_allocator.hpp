#ifndef __FAIR_ALLOCATOR_HPP__
#define __FAIR_ALLOCATOR_HPP__

#include <algorithm>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "allocator.hpp"
#include "resources.hpp"

namespace mesos { namespace internal { namespace fair_allocator {

using std::max;
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

  // Reload the FairAllocator's configuration file
  void reloadConfig();

  // Reload the configuration file if enough time has elapsed since the
  // last load and the file is modified on disk. Called periodically to
  // allow the config to be modified at runtime.
  void reloadConfigIfNecessary();
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


/**
 * Compares objects of type T for weighted Dominant Resource Fairness (DRF),
 * given functions to extract the resources, weight and ID of each object
 * as well as the total quantity of resources in the system.
 *
 * DrfComparators are used to compare both users and frameworks owned by a user.
 *
 * Objects are sorted into increasing order by their "score", which is defined
 * as dominant share / weight, and ties are broken by object IDs (e.g. users'
 * names) to get a deterministic sort.
 */
template<typename T>
struct DrfComparator
{
  typedef Resources (*ResourceGetter)(T t);
  typedef double (*WeightGetter)(T t);
  typedef string (*IdGetter)(T t);

  Resources total;
  ResourceGetter getResources;
  WeightGetter getWeight;
  IdGetter getId;
  
  DrfComparator(Resources _total,
                ResourceGetter _getResources,
                WeightGetter _getWeight,
                IdGetter _getId)
    : total(_total), getResources(_getResources),
      getWeight(_getWeight), getId(_getId)
  {
    if (total.cpus == 0) // Prevent division by zero if there are no slaves
      total.cpus = 1;
    if (total.mem == 0)
      total.mem = 1;
  }

  /**
   * Each T's score in the comparison is its dominant share divided by its
   * weight, so that if we sort by these scores, we get weighted DRF.
   */
  double getScore(T t) {
    Resources res = getResources(t);
    double dominantShare = max(res.cpus / (double) total.cpus,
                               res.mem  / (double) total.mem);
    return dominantShare / getWeight(t);
  }
  
  bool operator() (T t1, T t2)
  {
    double score1 = getScore(t1);
    double score2 = getScore(t2);
    if (score1 == score2)
      return getId(t1) < getId(t2); // Make sort deterministic for unit tests
    else
      return score1 < score2;
  }
};


}}} /* namespace */

#endif /* __FAIR_ALLOCATOR_HPP__ */
