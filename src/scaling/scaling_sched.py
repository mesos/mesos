#!/usr/bin/env python

import imp
import nexus
import os
import pickle
import sys
import time


class ScalingScheduler(nexus.Scheduler):
  def __init__(self, master):
    nexus.Scheduler.__init__(self)
    self.fid = -1
    self.tid = 0
    self.master = master
    self.running = {}

  def getFrameworkName(self, driver):
    return "Scaling Framework"

  def getExecutorInfo(self, driver):
    return nexus.ExecutorInfo("/root/nexus/src/scaling/scaling_exec", "", "")

  def registered(self, driver, fid):
    print "Scaling Scheduler Registered!"
    self.fid = fid

  def resourceOffer(self, driver, oid, offers):
    # Make sure the nested schedulers can actually run their tasks.
    # if len(offers) <= len(config.config) and len(config.config) != self.tid:
    #   print "Need at least one spare slave to do this work ... exiting!"
    #   driver.stop()
    #   return

    # Farm out the schedulers!
    tasks = []
    for offer in offers:
      if len(config.config) != self.tid:
        (todo, duration) = config.config[self.tid]
        print "Launching (%d, %d) as %d:%d" % (todo, duration, self.fid, self.tid)
        arg = pickle.dumps((self.master, (todo, duration)))
        task = nexus.TaskDescription(self.tid, offer.slaveId,
                                     "task %d" % self.tid, offer.params, arg)
        tasks.append(task)
        self.running[self.tid] = (todo, duration)
        self.tid += 1
    driver.replyToOffer(oid, tasks, {})

  def statusUpdate(self, driver, status):
    # For now, we are expecting our tasks to be lost ...
    if status.state == nexus.TASK_LOST:
      todo, duration = self.running[status.taskId]
      print "Finished %d todo at %d secs" % (todo, duration)
      del self.running[status.taskId]
      if self.tid == len(config.config) and len(self.running) == 0:
        driver.stop()
    elif status.state != nexus.TASK_RUNNING:
      print "Received unexpected status update for %d:%d" % (self.fid, status.taskId)


if __name__ == "__main__":
  # Load in the configuration.
  config = imp.load_source("config", sys.argv[1])

  if sys.argv[2] == "local" or sys.argv[2] == "localquiet":
    # Need more than one slave to do a scaling experiement ...
    print "Cannot do scaling experiments with 'local' or 'localquiet'!"
    sys.exit(1)

  nexus.NexusSchedulerDriver(ScalingScheduler(sys.argv[2]), sys.argv[2]).run()
