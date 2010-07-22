package mesos;

import java.util.Collection;


/**
 * Abstract class to be extended by all schedulers. 
 */
public abstract class Scheduler {
  public abstract String getFrameworkName(SchedulerDriver driver);
  public abstract ExecutorInfo getExecutorInfo(SchedulerDriver driver);
  public abstract void registered(SchedulerDriver driver, FrameworkID frameworkId);
  public abstract void resourceOffer(SchedulerDriver driver, OfferID offerId, Collection<SlaveOffer> offers);
  public abstract void offerRescinded(SchedulerDriver driver, OfferID offerId);
  public abstract void statusUpdate(SchedulerDriver driver, TaskStatus status);
  public abstract void frameworkMessage(SchedulerDriver driver, FrameworkMessage message);
  public abstract void slaveLost(SchedulerDriver driver, SlaveID slaveId);
  public abstract void error(SchedulerDriver driver, int code, String message);
}
