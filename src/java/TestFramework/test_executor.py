#!/usr/bin/env python
import nexus
import sys
import time
import os

from subprocess import *

class MyExecutor(nexus.Executor):
  def __init__(self):
    nexus.Executor.__init__(self)

  def init(self, driver, arg):
    print "in daemon executor"

  def launchTask(self, driver, task):
    time.sleep(3);
    update = nexus.TaskStatus(task.taskId, nexus.TASK_FINISHED, "")
    driver.sendStatusUpdate(update)

  def error(self, code, message):
    print "Error: %s" % message

if __name__ == "__main__":
  print "starting daemon framework executor"
  executor = MyExecutor()
  nexus.NexusExecutorDriver(executor).run()
