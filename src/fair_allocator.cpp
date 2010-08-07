#include <sys/stat.h>

#include <algorithm>

#include <glog/logging.h>
#include <tinyxml.h>

#include "fair_allocator.hpp"


using std::max;
using std::sort;
using std::string;
using std::vector;

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::master;
using namespace mesos::internal::fair_allocator;


namespace {
// Config file name; the file is looked for in Mesos's conf directory
const string CONFIG_FILE_NAME = "fair-allocator.xml";

// How often (in seconds) to check whether the config file is modified
// in order to load a new version of it
const double CONFIG_FILE_RELOAD_INTERVAL = 10.0;

// How long (in seconds) to wait after a config file is modified before
// trying to read it (so as not to load a file that is still being edited)
const double CONFIG_FILE_RELOAD_WAIT = 5.0;
}


FairAllocator::FairAllocator(Master* _master)
  : master(_master)
{
  // Load our configureation file
  reloadConfig();
  // Initialize reload variables to assume that this reload was successful
  // (so that there are reasonable values for all the times).
  time_t now = time(0);
  lastReloadAttempt = now;
  lastSuccessfulReload = now;
  lastReloadAttemptFailed = false;
}


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
  // Check whether we need to reload the config file, and do so if needed
  reloadConfigIfNecessary();

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


string FairAllocator::getConfigFile() {
  const Params& conf = master->getConf();
  string confDir = conf.get("conf", "");
  if (confDir == "") {
    return "";
  } else {
    return confDir + "/" + CONFIG_FILE_NAME;
  }
}


bool FairAllocator::reloadConfig()
{
  string confFile = getConfigFile();
  if (confFile == "") {
    LOG(WARNING) << "Cannot locate FairAllocator config file, so no "
                 << "external configuration will be used";
  }

  // Read a document from the file
  TiXmlDocument doc(confFile);
  if (!doc.LoadFile()) {
    LOG(ERROR) << "Failed to load XML file " << confFile
               << ", so no FairAllocator options will be read";
    return false;
  }

  // Get the root element and check that it is valid
  TiXmlElement* root = doc.RootElement();
  if (root == NULL) {
    LOG(ERROR) << "Malformed FairAllocator config file: no root element";
    return false;
  }
  if (root->ValueStr() != "configuration") {
    LOG(ERROR) << "Malformed FairAllocator config file: "
               << "root element is not <configuration>";
    return false;
  }
  TiXmlHandle hRoot(root);

  // Get the <users> element within root, which should contain a <user> child
  // for each user we want to configure, and load user weights from them.
  // We first put the weights into a temporary map and only "commit" at the
  // end, in case we run into a parsing error later in the load process.
  unordered_map<string, double> newWeights;
  TiXmlElement* users = hRoot.FirstChildElement("users").ToElement();
  if (users != NULL) {
    for (TiXmlElement* user = users->FirstChild("user"); user != NULL;
         user = user->NextSibling("user")) {
      const char* name = user->Attribute("name");
      if (name == NULL) {
        LOG(ERROR) << "Malformed FairAllocator config file: "
                   << "user without name at line " << user->Row();
        return false;
      } else {
        // Check for a weight
        TiXmlText* text = user.FirstChild("weight").FirstChild().ToText();
        if (text != NULL) {
          try {
            newWeights[name] = lexical_cast<double>(text.ValueStr());
          } catch(bad_lexical_cast& e) {
            LOG(ERROR) << "Malformed FairAllocator config file: "
                       << "bad value for <weight> at line " << text->Row();
            return false;
          }
        }
      }
    }
  }

  // Everything loaded successfully; commit the changed settings
  
  // Commit weights; first reset everyone's weight to 1, then set it to\
  // whatever we loaded in from the file
  foreachpair (const std::string& name, UserInfo* info, userInfos) {
    info->weight = 1.0;
  }
  foreachpair (const std::string& name, double newWeight, newWeights) {
    getUserInfo(name)->weight = newWeight;
  }

  return true;
}


void FairAllocator::reloadConfigIfNecessary()
{
  string confFile = getConfigFile();
  if (confFile == "") {
    return; // No config file set
  }
  time_t now = time(0);
  if (difftime(now, lastReloadAttempt) >= CONFIG_FILE_RELOAD_INTERVAL) {
    // Enough time has passed since last reload check; see if file is modified
    lastReloadAttempt = now;
    struct stat st;
    if (stat(confFile.c_str(), &st) != 0) {
      // Stat failed, so file is inaccessible; do not attempt to load it,
      // but log a message unless we have done so before
      if (!lastReloadAttemptFailed) {
        PLOG(ERROR) << "Stat failed on " << confFile;
      }
      lastReloadAttemptFailed = true;
      return;
    }
    time_t modTime = st.st_mtime;
    if (difftime(modTime, lastReloadAttempt) > 0 &&
        difftime(now, modTime) > CONFIG_FILE_RELOAD_WAIT) {
      // File was modified since last reload, but not too recently
      // (so editing is probably not still in progress); attempt a reload
      if (reloadConfig()) {
        lastSuccessfulReload = now;
        lastReloadAttemptFailed = false;
      } else {
        // Log a message the first time a reload fails
        if (!lastReloadAttemptFailed) {
          LOG(ERROR) << "Reloading config file failed";
        }
        lastReloadAttemptFailed = true;
      }
    }
  }
}


void UserInfo::updateResources()
{
  Resources res;
  foreach (Framework* fw, frameworks) {
    res += fw->resources;
  }
  this->resources = res;
}
