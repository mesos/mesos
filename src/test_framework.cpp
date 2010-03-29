#include <iostream>
#include <stdio.h>
#include <nexus_sched.h>

using namespace std;

void registered(nexus_sched *sched, framework_id fid)
{
  cout << "Registered with Nexus, framework ID = " << fid << endl;
}


void slot_offer(nexus_sched *sched, offer_id oid,
                nexus_slot *slots, int num_slots)
{
  // TODO: Make this loop over offers rather than looking only at first offer!
  static int tasksStarted = 0;
  cout << "Got slot offer " << oid << endl;
  if (tasksStarted > 4) {
    cout << "Refusing it" << endl;
    nexus_sched_reply_to_offer(sched, oid, 0, 0, "timeout=-1");
  } else {
    task_id tid = tasksStarted++;
    cout << "Accepting it to start task " << tid << endl;
    nexus_task_desc desc = { tid, slots[0].sid, "task",
      "cpus=1\nmem=33554432", 0, 0 };
    nexus_sched_reply_to_offer(sched, oid, &desc, 1, "");
    if (tasksStarted > 4)
      nexus_sched_unreg(sched);
  }
}


void slot_offer_rescinded(nexus_sched *sched, offer_id oid)
{
  cout << "Slot offer rescinded: " << oid << endl;
}


void status_update(nexus_sched *sched, nexus_task_status *status)
{
  cout << "Task " << status->tid << " entered state " << status->state << endl;
}


void framework_message(nexus_sched *sched, nexus_framework_message *msg)
{
  cout << "Got a framework message from slave " << msg->sid
       << ": " << (char *) msg->data << endl;
}


void slave_lost(nexus_sched *sched, slave_id sid)
{
  cout << "Lost slave " << sid << endl;
}


void error(nexus_sched *sched, int code, const char *message)
{
  cout << "Error from Nexus: " << message << endl;
}





int main(int argc, char **argv)
{
  // Setup the scheduler.
  nexus_sched sched;

  sched.framework_name = "test framework";
  ""; // Executor (will be set in main to get absolute path);

  sched.registered = registered;
  sched.slot_offer = slot_offer;
  sched.slot_offer_rescinded = slot_offer_rescinded;
  sched.status_update = status_update;
  sched.framework_message = framework_message;
  sched.slave_lost = slave_lost;
  sched.error = error;

  sched.data = NULL;

  // Setup the executor info.
  char cwd[512];
  getcwd(cwd, sizeof(cwd));
  sched.exec_info.uri = (string(cwd) + "/test-executor").c_str();

  sched.exec_info.dir = "";

  sched.exec_info.data = (void *) "test";
  sched.exec_info.data_len = 4;

  if (nexus_sched_init(&sched) < 0) {
    perror("nexus_sched_init");
    return -1;
  }

  if (nexus_sched_reg(&sched, argv[1]) < 0) {
    perror("nexus_sched_reg");
    return -1;
  }

  nexus_sched_join(&sched);

  if (nexus_sched_destroy(&sched) < 0) {
    perror("nexus_sched_destroy");
    return -1;
  }

  return 0;
}
