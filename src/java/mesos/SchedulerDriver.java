package mesos;

import java.util.Collection;
import java.util.Map;


public abstract class SchedulerDriver {

  // Lifecycle methods.
  public abstract int start();

  public abstract int stop();

  public abstract int join();

  public int run() {
    System.out.println("SchedulerDriver.run()");
    int ret = start();
    return ret != 0 ? ret : join();
  }

  // Communication methods.
  public abstract int sendFrameworkMessage(FrameworkMessage message);

  public abstract int killTask(TaskID taskId);

  public abstract int replyToOffer(OfferID offerId,
                                   Collection<TaskDescription> tasks,
                                   Map<String, String> params);

  public abstract int reviveOffers();

  public abstract int sendHints(Map<String, String> hints);
};
