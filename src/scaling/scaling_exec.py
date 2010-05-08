#!/usr/bin/env python
import nexus
import os
import pickle
import sys


class NestedScheduler(nexus.Scheduler):
  def __init__(self, todo, duration, executor):
    nexus.Scheduler.__init__(self)
    self.fid = -1
    self.tid = 0
    self.todo = todo
    self.duration = duration
    self.executor = executor
    self.finished = 0
    self.slaves = []
    self.running = {}

  def getFrameworkName(self, driver):
    return "Nested Framework: %d todo at %d secs" % (self.todo, self.duration)

  def getExecutorInfo(self, driver):
    return nexus.ExecutorInfo("/root/nexus/src/scaling/nested_exec", "", "")

  def registered(self, driver, fid):
    self.fid = fid
    print "Nested Scheduler Registered!"

  def resourceOffer(self, driver, oid, offers):
    tasks = []
    for offer in offers:
      # Only run one task on a machine because tasks will get
      # serialized since NestedExecutor doesn't use threads.
      if offer.slaveId in self.slaves:
        continue
      if self.todo == -1 or self.todo != self.tid:
        print "Launching %d:%d on %d" % (self.fid, self.tid, offer.slaveId)
        params = { "cpus": "%d" % 4, "mem": "%d" % (4 * 1024 * 1024 * 1024) }
        task = nexus.TaskDescription(self.tid, offer.slaveId,
                                     "nested %d:%d" % (self.fid, self.tid),
                                     params,
                                     "%d" % self.duration)
        self.slaves.append(offer.slaveId)
        self.running[task.taskId] = offer.slaveId
        tasks.append(task)
        self.tid += 1
        #msg = nexus.FrameworkMessage(-1, , "")
        #executor.sendFrameworkMessage("")
    driver.replyToOffer(oid, tasks, {})

  def statusUpdate(self, driver, status):
    if status.state == nexus.TASK_FINISHED or status.state == nexus.TASK_LOST:
      self.finished += 1
      slaveId = self.running[status.taskId]
      self.slaves.remove(slaveId)
      del self.running[status.taskId]
    if self.finished == self.todo:
      print "All nested tasks done, stopping scheduler!"
      # driver.stop()
      # self.executor.stop()
      sys.exit(0)

class ScalingExecutor(nexus.Executor):
  def __init__(self):
    nexus.Executor.__init__(self)
    self.tid = -1
    self.nested_driver = -1

  def launchTask(self, driver, task):
    self.tid = task.taskId
    master, (todo, duration) = pickle.loads(task.arg)
    scheduler = NestedScheduler(todo, duration, self)
    self.nested_driver = nexus.NexusSchedulerDriver(scheduler, master)
    self.nested_driver.start()

  def killTask(self, driver, tid):
    if (tid != self.tid):
      print "Expecting different task id ... killing anyway!"
    if self.nested_driver != -1:
      self.nested_driver.stop()
      self.nested_driver.join()
    driver.sendStatusUpdate(nexus.TaskStatus(tid, nexus.TASK_KILLED, ""))

  def shutdown(self, driver):
    self.killTask(self.tid)

  def error(self, driver, code, message):
    print "Error: %s" % message


if __name__ == "__main__":
  nexus.NexusExecutorDriver(ScalingExecutor()).run()
