#!/usr/bin/env python

import boto
import datetime
import httplib
import mesos
import os
import Queue
import sys
import threading
import time

from optparse import OptionParser
from socket import gethostname
from subprocess import *

MIN_SERVERS = 1
LOAD_BALANCER_NAME = "my-load-balancer"
TARGET_CONN_PER_MIN_PER_BACKEND = 25 * 60 #This is probably still a bit too low

class ApacheWebFWScheduler(mesos.Scheduler):
  def __init__(self):
    mesos.Scheduler.__init__(self)
    self.lock = threading.RLock()
    self.id = 0
    self.elb = -1
    self.reconfigs = 0
    self.servers = {}
    self.overloaded = False
    self.desired_servers = 1
    #AWS environment has to be set up
    #either using keypairs or x.509 certificates
    self.cw_conn = boto.connect_cloudwatch()
    self.metrics = self.cw_conn.list_metrics()
    print self.metrics[13]
    self.host_map = updated_host_map()
    self.elb_conn = boto.connect_elb()

  def registered(self, driver, fid):
    print "Mesos elb+apache scheduler registered as framework #%s" % fid
    self.driver = driver

  def getFrameworkName(self, driver):
      return "elb+apache"

  def getExecutorInfo(self, driver):
    execPath = os.path.join(os.getcwd(), "startapache.sh")
    return mesos.ExecutorInfo(execPath, "")

  def resourceOffer(self, driver, oid, slave_offers):
    print "Got resource offer %s with %s slots." % (oid, len(slave_offers))
    self.lock.acquire()
    tasks = []
    for offer in slave_offers:
      if offer.host in self.servers.values():
        print "Rejecting slot on host " + offer.host + " because we've launched a server on that machine already."
        #print "self.servers currently looks like: " + str(self.servers)
      elif len(self.servers) >= self.desired_servers and len(self.servers) > 0:
        print "Rejecting slot because we've launched enough tasks."
      elif int(offer.params['mem']) < 1024:
        print "Rejecting offer because it doesn't contain enough memory (it has " + offer.params['mem'] + " and we need 1024mb."
      elif int(offer.params['cpus']) < 1:
        print "Rejecting offer because it doesn't contain enough CPUs."
      else:
        print "len(self.servers) = " + str(len(self.servers)) + ", self.desired_servers  = " + str(self.desired_servers) + ", len(self.servers) = " + str(len(self.servers))
        print "Offer is for " + offer.params['cpus'] + " CPUS and " + offer.params["mem"] + " MB on host " + offer.host
        params = {"cpus": "1", "mem": "1024"}
        td = mesos.TaskDescription(self.id, offer.slaveId, "server %s" % self.id, params, "")
        print "Accepting task, id=" + str(self.id) + ", params: " + params['cpus'] + " CPUS, and " + params['mem'] + " MB, on node " + offer.host
        tasks.append(td)
        self.servers[self.id] = offer.host
        self.id += 1
        print "self.servers length is now " + str(len(self.servers))
    driver.replyToOffer(oid, tasks, {"timeout":"1"})
    #driver.replyToOffer(oid, tasks, {})
    print "done with resourceOffer()"
    self.lock.release()

  def statusUpdate(self, driver, status):
    print "received status update from taskID " + str(status.taskId) + ", with state: " + str(status.state)
    reconfigured = False
    self.lock.acquire()
    if status.taskId in self.servers.keys():
      if status.state == mesos.TASK_STARTING:
        print "Task " + str(status.taskId) + " reported that it is STARTING."
        del self.servers[status.taskId]
      if status.state == mesos.TASK_RUNNING:
        print "Task " + str(status.taskId) + " reported that it is RUNNING, reconfiguring elb to include it in webfarm now."
        print "Adding task's host node to load balancer " + LOAD_BALANCER_NAME
        host_name = self.servers[status.taskId]
        print "Task's hostname is " + host_name
        print "self.host_map is " + str(self.host_map)
        instance_id = self.host_map[host_name]
        print "Task's instance id is " + instance_id
        lbs = self.elb_conn.register_instances(LOAD_BALANCER_NAME, [instance_id])
        #lbs = self.elb_conn.register_instances("my-load-balancer","i-5dd18037")
        print "Load balancer reported all backends as: " + str(lbs)
      if status.state == mesos.TASK_FINISHED:
        del self.servers[status.taskId]
        print "Task " + str(status.taskId) + " reported FINISHED (state " + status.state + ")."
      if status.state == mesos.TASK_FAILED:
        print "Task " + str(status.taskId) + " reported that it FAILED!"
        del self.servers[status.taskId]
      if status.state == mesos.TASK_KILLED:
        print "Task " + str(status.taskId) + " reported that it was KILLED!"
        del self.servers[status.taskId]
      if status.state == mesos.TASK_LOST:
        print "Task " + str(status.taskId) + " reported was LOST!"
        del self.servers[status.taskId]
    self.lock.release()
    print "done in statusupdate"

#  def scaleUp(self):
#    print "SCALING UP"
#    self.lock.acquire()
#    self.overloaded = True
#    self.lock.release()
#
#  def scaleDown(self, id):
#    print "SCALING DOWN (removing server %d)" % id
#    kill = False
#    self.lock.acquire()
#    if self.overloaded:
#      self.overloaded = False
#    else:
#      kill = True
#    self.lock.release()
#    if kill:
#      self.driver.killTask(id)


def updated_host_map():
  conn = boto.connect_ec2()
  reservations = conn.get_all_instances()
  i = [i.instances for i in reservations]
  instances = [item for sublist in i for item in sublist]
  return dict([(str(i.private_dns_name), str(i.id)) for i in instances])


def monitor(sched):
  print "in MONITOR()"
  while True:
    #ELB only reports "metrics" every minute at its most fine granularity
    time.sleep(2)
    print "done sleeping"
    try:
      #looking for the RequestCount metric for our load balancer 
      request_count = sched.metrics[13]
      print request_count
      result = request_count.query(datetime.datetime.now()-datetime.timedelta(minutes=2),datetime.datetime.now(), 'Sum', 'Count', 60)
      print result
      if len(result) == 0:
        sched.desired_servers = 1
        print "RequestCount was 0, so set sched.desired_servers to " + str(sched.desired_servers)
      else:
        #TODO(andyk): Probably want to weight this to smooth out ups and downs
        new_num_servers = result[0]["Sum"]/TARGET_CONN_PER_MIN_PER_BACKEND
        sched.desired_servers = max(new_num_servers,1)
        print "RequestCount was " + str(result[0]["Sum"]) + ", sched.desired_servers to " + str(sched.desired_servers)
       
#        if int(data[33]) >= START_THRESHOLD:
#          sched.scaleUp()
#        elif int(data[4]) <= KILL_THRESHOLD:
#          minload, minid = (sys.maxint, 0)
#          for l in lines:
#            cols = l.split(',')
#            id = int(cols[1])
#            load = int(cols[4])
#            if load < minload:
#              minload = load
#              minid = id
#
#          if len(lines) > MIN_SERVERS and minload == 0:
#            sched.scaleDown(minid)
#
#        conn.close()
    except Exception, e:
      print "exception in monitor()" + str(e)
      continue
  print "done in MONITOR()"

if __name__ == "__main__":
  parser = OptionParser(usage = "Usage: %prog mesos_master")

  (options,args) = parser.parse_args()
  if len(args) < 1:
    print >> sys.stderr, "At least one parameter required."
    print >> sys.stderr, "Use --help to show usage."
    exit(2)

  print "sched = ApacheWebFWScheduler()"
  sched = ApacheWebFWScheduler()

  print "Connecting to mesos master %s" % args[0]
  driver = mesos.MesosSchedulerDriver(sched, sys.argv[1])

  threading.Thread(target = monitor, args=[sched]).start()

  driver.run()


  print "Scheduler finished!"
