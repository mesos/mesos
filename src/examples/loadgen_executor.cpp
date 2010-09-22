#include <time.h>

#include <mesos_exec.hpp>

#include <cstdlib>
#include <iostream>
#include <sstream>

using namespace std;
using namespace mesos;


class MemHogExecutor;


struct TaskArg
{
  MemHogExecutor* executor;
  TaskID taskId;
  int numThreads;
  int64_t memToHog; // in bytes
  int64_t numSteps;

  TaskArg(MemHogExecutor* executor_, TaskID taskId_, int numThreads_,
            int64_t memToHog_, int64_t numSteps_)
    : executor(executor_), numThreads(numThreads_), taskId(taskId_),
      memToHog(memToHog_), numSteps(numSteps_) {}
};


struct ThreadArg
{
  int threadId;
  int64_t numSteps;
  int64_t memToHog; // in bytes
  char* mem;
  pthread_mutex_t* lock;
  pthread_cond_t* cond;
  bool* flag;

  ThreadArg(int i, int64_t ns, int64_t mth, char* m, pthread_mutex_t* l, pthread_cond_t* c, bool* f)
    : threadId(i), numSteps(ns), memToHog(mth), mem(m), lock(l), cond(c), flag(f) {}
};


void* runTask(void* taskArg);
void* runThread(void* threadArg);


class MemHogExecutor : public Executor
{
public:
  ExecutorDriver* driver;

  virtual ~MemHogExecutor() {}

  virtual void init(ExecutorDriver* driver, const ExecutorArgs &args) {
    this->driver = driver;
  }

  virtual void launchTask(ExecutorDriver*, const TaskDescription& task) {
    cout << "Executor starting task " << task.taskId << endl;
    int64_t memToHog;
    int64_t numSteps;
    int numThreads;
    istringstream in(task.arg);
    in >> memToHog >> numSteps >> numThreads;
    memToHog *= 1024LL * 1024LL; // Convert from MB to bytes
    TaskArg* arg = new TaskArg(this, task.taskId, numThreads, memToHog, numSteps);
    pthread_t thread;
    pthread_create(&thread, 0, runTask, arg);
    pthread_detach(thread);
  }
};


// A simple linear congruential generator, used to access memory in a random
// pattern without relying on a possibly synchronized stdlib rand().
// Constants from http://en.wikipedia.org/wiki/Linear_congruential_generator.
uint32_t nextRand(uint32_t x) {
  const int64_t A = 1664525;
  const int64_t B = 1013904223;
  int64_t longX = x;
  return (uint32_t) ((A * longX + B) & 0xFFFFFFFF);
}


// Function executed by each task.
void* runTask(void* taskArg)
{
  TaskArg* arg = (TaskArg*) taskArg;
  cout << "Running a worker thread..." << endl;
  char* data = new char[arg->memToHog];
  bool* done = new bool[arg->numThreads];
  pthread_mutex_t* locks = new pthread_mutex_t[arg->numThreads];
  pthread_cond_t* conds = new pthread_cond_t[arg->numThreads];
  // Create a bunch of stuff
  for (int i = 0; i < arg->numThreads; i++) {
    done[i] = false;
    pthread_mutex_init(&locks[i], NULL);
    pthread_cond_init(&conds[i], NULL);
  }
  // Launch threads
  for (int i = 0; i < arg->numThreads; i++) {
    // int64_t ns, int64_t mth, char* m, pthread_mutex_t* l, pthread_cond_t* c, bool* f
    ThreadArg* ta = new ThreadArg(i, arg->numSteps, arg->memToHog, data,
        &locks[i], &conds[i], &done[i]);
    pthread_t thread;
    pthread_create(&thread, 0, runThread, ta);
    pthread_detach(thread);
  }
  // Wait for them to finish
  for (int i = 0; i < arg->numThreads; i++) {
    pthread_mutex_lock(&locks[i]);
    while (!done[i]) {
      cout << "Waiting on " << i << endl;
      pthread_cond_wait(&conds[i], &locks[i]);
    }
    pthread_mutex_unlock(&locks[i]);
    cout << "OMG, it's done" << endl;
  }
  // Destroy stuff
  for (int i = 0; i < arg->numThreads; i++) {
    pthread_mutex_destroy(&locks[i]);
    pthread_cond_destroy(&conds[i]);
  }
  delete[] data;
  delete[] locks;
  delete[] conds;
  // Send status update
  TaskStatus status(arg->taskId, TASK_FINISHED, "");
  arg->executor->driver->sendStatusUpdate(status);
  delete arg;
  return 0;
}


// Function executed by each worker thread.
void* runThread(void* threadArg)
{
  ThreadArg* arg = (ThreadArg*) threadArg;
  cout << "Running a worker thread..." << endl;
  int32_t count = 0;
  uint32_t pos = arg->threadId;
  while (true) {
    pos = nextRand(pos);
    arg->mem[pos % arg->memToHog] = pos;
    count++;
    if (count == arg->numSteps) {
      // We're done!
      cout << "Ending a worker thread..." << endl;
      pthread_mutex_lock(arg->lock);
      *(arg->flag) = true;
      pthread_cond_signal(arg->cond);
      pthread_mutex_unlock(arg->lock);
      delete arg;
      return 0;
    }
  }
}


int main(int argc, char** argv) {
  MemHogExecutor exec;
  MesosExecutorDriver driver(&exec);
  driver.run();
  return 0;
}
