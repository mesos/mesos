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
TARGET_CONN_PER_MIN_PER_BACKEND = 5 * 60 #This is probably still a bit too low

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
    self.kill_list = [] # a list of task ids we called killTask on but haven't 
                        # received a status update with status = KILLEd yet for
    self.elb_conn = boto.connect_elb()
    #reset the load balancer to have 0 back ends registered
    self.elb_conn.deregister_instances(LOAD_BALANCER_NAME, 
                                       self.host_map.values())

  def registered(self, driver, fid):
    print "Mesos elb+apache scheduler registered as framework #%s" % fid
    self.driver = driver

  def getFrameworkName(self, driver):
      return "elb+apache"

  def getExecutorInfo(self, driver):
    execPath = os.path.join(os.getcwd(), "startapache.sh")
    return mesos.ExecutorInfo(execPath, "")

  def resourceOffer(self, driver, oid, slave_offers):
    print "\nGot resource offer %s with %s slots." % (oid, len(slave_offers))
    self.lock.acquire()
    tasks = []
    nodes_used, no_more_needed = 0, 0
    for offer in slave_offers:
      if offer.host in self.servers.values():
        nodes_used += 1
      elif len(self.servers) >= self.desired_servers and len(self.servers) > 0:
        no_more_needed += 1
      elif int(offer.params['mem']) < 1024:
        print "Rejecting offer because it doesn't contain enough memory" + \
              "(it has " + offer.params['mem'] + " and we need 1024mb."
      elif int(offer.params['cpus']) < 1:
        #print "Rejecting offer because it doesn't contain enough CPUs."
        pass
      else:
        print "len(self.servers) = " + str(len(self.servers)) + \
              ", self.desired_servers  = " + str(self.desired_servers)
        print "Offer is for " + offer.params['cpus'] + " CPUS and " + \
              offer.params["mem"] + " MB on host " + offer.host
        params = {"cpus": "1", "mem": "1024"}
        td = mesos.TaskDescription(self.id, offer.slaveId,
                                   "server %s" % self.id, params, "")
        print "Accepting task, id=" + str(self.id) + ", params: " + \
              params['cpus'] + " CPUS, and " + params['mem'] + \
              " MB, on node " + offer.host
        tasks.append(td)
        self.servers[self.id] = offer.host
        self.id += 1
        print "self.servers length is now " + str(len(self.servers))
    driver.replyToOffer(oid, tasks, {"timeout":"1"})
    if nodes_used > 0:
      print ("Rejecting %d slots because we've launched servers on those " + \
             "machines already.") % nodes_used
    if no_more_needed > 0:
      print "Rejecting %d slot because we've launched enough tasks." % \
            no_more_needed
    print "Done with resourceOffer()"
    self.lock.release()

  def statusUpdate(self, driver, status):
    print "\nReceived status update from taskID " + str(status.taskId) + \
          ", with state: " + str(status.state)
    self.lock.acquire()
    if not status.taskId in self.servers.keys():
      print "This status was from a node where the server wasn't " + \
            "(supposed to be) running."
    else:
      print "Parsing and handling status update."
      if status.state == mesos.TASK_STARTING:
        print "Task " + str(status.taskId) + " reported that it's STARTING."
        del self.servers[status.taskId]
      if status.state == mesos.TASK_RUNNING:
        print ("Task %s reported that it's RUNNING, reconfiguring elb to " + 
              "include it in webfarm now.") % str(status.taskId)
        print "Adding task's host node to load balancer %s." % \
              LOAD_BALANCER_NAME
        host_name = self.servers[status.taskId]
        print "Task's hostname is %s." % host_name
        instance_id = self.host_map[host_name]
        print "Task's instance id is " + instance_id
        lbs = self.elb_conn.register_instances(LOAD_BALANCER_NAME, 
                                               [instance_id])
        print "Load balancer reported all backends as: %s." % str(lbs)
      if status.state == mesos.TASK_FINISHED:
        del self.servers[status.taskId]
        print "Task %s reported FINISHED (state %s)." % \
              str(status.taskId), str(status.state)
      if status.state == mesos.TASK_FAILED:
        print "Task %s reported that it FAILED!" % str(status.taskId)
        del self.servers[status.taskId]
      if status.state == mesos.TASK_KILLED:
        print "Task %s reported that it was KILLED!" % str(status.taskId)
        del self.servers[status.taskId]
      if status.state == mesos.TASK_LOST:
        print "Task %s was reported as LOST!" % str(status.taskId)
        del self.servers[status.taskId]
    self.lock.release()
    print "Done in statusupdate()."

  def kill_backends(self, num):
    print "In kill_backends(), killing %i backends" % num
    host_names, host_ids = [], []
    for m,n in self.servers.items()[:num]:
      host_ids.append(m)
      host_names.append(n)
    print "host_names[] is " + str(host_names) + "."
    instance_ids = [self.host_map[i] for i in host_names]
    print "Deregistering instance ids from elb: " + str(instance_ids) + "."
    lbs = self.elb_conn.deregister_instances(LOAD_BALANCER_NAME, instance_ids)
    print "Calling driver.kill_task on tasks: " + str(host_ids) + "."
    [self.driver.killTask(i) for i in host_ids]
    [self.kill_list.append(i) for i in host_ids]
    #The following (i.e. removing host from self.servers) will happen when the
    #task's status is reported to the FW as KILLED
    #print "removing task from servers list: " + str(host_ids) 
    #[self.servers.pop(int(i)) for i in host_ids]


def updated_host_map():
  conn = boto.connect_ec2()
  reservations = conn.get_all_instances()
  i = [i.instances for i in reservations]
  instances = [item for sublist in i for item in sublist]
  return dict([(str(i.private_dns_name), str(i.id)) for i in instances])


def monitor(sched):
  while True:
    #ELB only reports "metrics" every minute at its most fine granularity
    time.sleep(2)
    print "\nIn monitor() loop. Done sleeping, acquiring lock."
    sched.lock.acquire()
    try:
      #get the RequestCount metric for our load balancer 
      rct = [m for m in sched.metrics if str(m) == "Metric:RequestCount"][0]
      result = rct.query(datetime.datetime.now()-datetime.timedelta(minutes=1),
                                   datetime.datetime.now(), 'Sum', 'Count', 60)
      print "Request count query returned: %s" % result
      if len(result) == 0:
        sched.desired_servers = 1
        print "RequestCount was 0, so set sched.desired_servers to " + \
              str(sched.desired_servers)
      else:
        #TODO(andyk): Probably want to weight this to smooth out ups and downs
        r = max(result, key=lambda x: x["Timestamp"])
        new_num_servers = int(r["Sum"] / TARGET_CONN_PER_MIN_PER_BACKEND)
        sched.desired_servers = max(new_num_servers,1)
        print "RequestCount was " + str(result[0]["Sum"]) + \
              ", setting sched.desired_servers = " + str(sched.desired_servers) 
      print "len(sched.servers) is " + str(len(sched.servers))
      if sched.desired_servers < len(sched.servers):
        print "Time to kill some servers"
        num_to_kill = len(sched.servers)-sched.desired_servers
        print "Removing %i backends" % num_to_kill
        sched.kill_backends(num_to_kill)
    except Exception, e:
      print "Exception in monitor()" + str(e)
      continue
    print "Done with monitor() loop, releasing lock."
    sched.lock.release()

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
