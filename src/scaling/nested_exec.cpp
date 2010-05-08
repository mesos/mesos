#include <nexus_exec.hpp>

#include <boost/lexical_cast.hpp>

#include <iostream>


using namespace nexus;

using boost::lexical_cast;


class NestedExecutor : public Executor
{
private:
  FrameworkID fid;
  TaskID tid;

public:
  NestedExecutor() {}
  virtual ~NestedExecutor() {}

  virtual void init(ExecutorDriver *driver, const ExecutorArgs &args)
  {
    fid = args.frameworkId;
  }

  virtual void launchTask(ExecutorDriver* d, const TaskDescription& task)
  {
    tid = task.taskId;
    double duration = lexical_cast<double>(task.arg);
    std::cout << "(" << fid << ":" << tid << ") Sleeping for "
	      << duration << " seconds." << std::endl;
    // TODO(benh): Don't sleep, this blocks the event loop!
    sleep(duration);
    // HACK: Stopping executor to free resources instead of doing TASK_FINISHED.
    exit(0); // driver.stop();
    // status = nexus.TaskStatus(self.tid, nexus.TASK_FINISHED, "")
    // driver.sendStatusUpdate(status)
  }
};


int main(int argc, char** argv)
{
  NestedExecutor exec;
  NexusExecutorDriver driver(&exec);
  driver.run();
  return 0;
}
