#include <mesos_sched.hpp>

#include <libgen.h>

#include <cstdlib>
#include <iostream>
#include <sstream>

#include <boost/lexical_cast.hpp>

#include "foreach.hpp"

using namespace std;
using namespace mesos;

using boost::lexical_cast;


class MyScheduler : public Scheduler
{
  string executor;
  int64_t numSteps;
  int threadsPerTask;
  int memToHog;
  int tasksLaunched;
  int tasksFinished;
  int totalTasks;

public:
  MyScheduler(const string& executor_, int totalTasks_, int64_t numSteps_,
      int threadsPerTask_, int64_t memToHog_)
    : executor(executor_), totalTasks(totalTasks_), numSteps(numSteps_),
      threadsPerTask(threadsPerTask_), memToHog(memToHog_),
      tasksLaunched(0), tasksFinished(0) {}

  virtual ~MyScheduler() {}

  virtual string getFrameworkName(SchedulerDriver*) {
    return "Memory hog";
  }

  virtual ExecutorInfo getExecutorInfo(SchedulerDriver*) {
    return ExecutorInfo(executor, "");
  }

  virtual void registered(SchedulerDriver*, FrameworkID fid) {
    cout << "Registered!" << endl;
  }

  virtual void resourceOffer(SchedulerDriver* d,
                             OfferID id,
                             const vector<SlaveOffer>& offers) {
    vector<TaskDescription> tasks;
    foreach (const SlaveOffer &offer, offers) {
      // This is kind of ugly because operator[] isn't a const function
      int cpus = lexical_cast<int>(offer.params.find("cpus")->second);
      int mem = lexical_cast<int>(offer.params.find("mem")->second);
      while ((tasksLaunched < totalTasks) &&
             (cpus >= threadsPerTask) && (mem >= memToHog)) {
        TaskID tid = tasksLaunched++;
        cout << "Launcing task " << tid << " on " << offer.host << endl;
        map<string, string> params;
        params["cpus"] = lexical_cast<string>(threadsPerTask);
        params["mem"] = lexical_cast<string>(memToHog);
        ostringstream arg;
        arg << memToHog << " " << numSteps << " " << threadsPerTask;
        TaskDescription desc(tid, offer.slaveId, "task", params, arg.str());
        tasks.push_back(desc);
        cpus -= threadsPerTask;
        mem -= memToHog;
      }
    }
    map<string, string> params;
    params["timeout"] = "-1";
    d->replyToOffer(id, tasks, params);
  }

  virtual void statusUpdate(SchedulerDriver* d, const TaskStatus& status) {
    cout << "Task " << status.taskId << " is in state " << status.state << endl;
    if (status.state == TASK_LOST)
      cout << "Task " << status.taskId << " lost. Not doing anything about it." << endl;
    if (status.state == TASK_FINISHED)
      tasksFinished++;
    if (tasksFinished == totalTasks)
      d->stop();
  }
};


int main(int argc, char ** argv) {
  if (argc != 6) {
    cerr << "Usage: " << argv[0]
         << " <master> <tasks> <steps (millions)> <threads_per_task>"
         << " <MB_per_task>" << endl;
    return -1;
  }
  // Find this executable's directory to locate executor
  char buf[4096];
  realpath(dirname(argv[0]), buf);
  string executor = string(buf) + "/loadgen-executor";
  MyScheduler sched(executor,
                    lexical_cast<int>(argv[2]),
                    lexical_cast<int64_t>(argv[3]) * 1000 * 1000,
                    lexical_cast<int>(argv[4]),
                    lexical_cast<int64_t>(argv[5]));
  MesosSchedulerDriver driver(&sched, argv[1]);
  driver.run();
  return 0;
}
