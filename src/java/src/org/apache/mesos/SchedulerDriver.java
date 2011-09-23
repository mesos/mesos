package org.apache.mesos;

import org.apache.mesos.Protos.*;

import java.util.Collection;
import java.util.Map;


public interface SchedulerDriver {
  // Lifecycle methods.
  public Status start();
  public Status stop();
  public Status stop(boolean failover);
  public Status abort();
  public Status join();
  public Status run();

  // Communication methods.
  public Status sendFrameworkMessage(SlaveID slaveId, ExecutorID executorId, byte[] data);
  public Status killTask(TaskID taskId);
  public Status launchTasks(OfferID offerId, Collection<TaskDescription> tasks, Filters filters);
  public Status launchTasks(OfferID offerId, Collection<TaskDescription> tasks);
  public Status reviveOffers();
};
