package mesos;

import java.util.Collection;
import java.util.Map;


/**
 * Concrete implementation of SchedulerDriver that communicates with
 * a Mesos master.
 */
public class MesosSchedulerDriver extends SchedulerDriver {

  static {
    System.loadLibrary("mesos");
  }

  public MesosSchedulerDriver(Scheduler sched, String url,
                              FrameworkID frameworkId) {
    this.sched = sched;
    this.url = url;
    this.frameworkId = frameworkId;

    initialize();
  }

  public MesosSchedulerDriver(Scheduler sched, String url) {
    this(sched, url, FrameworkID.EMPTY);
  }

  private native void initialize();

  public native int start();
  public native int stop();
  public native int join();

  public native int sendFrameworkMessage(FrameworkMessage message);

  public native int killTask(TaskID taskId);

  public native int replyToOffer(OfferID offerId,
                                 Collection<TaskDescription> tasks,
                                 Map<String, String> params);

  public native int reviveOffers();

  public native int sendHints(Map<String, String> hints);

  private Scheduler sched;
  private String url;
  private FrameworkID frameworkId;

  private long __sched;
  private long __driver;
};
