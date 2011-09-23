import java.io.File;

import java.util.List;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;


public class TestExceptionFramework {
  static class MyScheduler implements Scheduler {
    public MyScheduler() {
    }

    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkId) {
      throw new ArrayIndexOutOfBoundsException();
    }

    @Override
    public void resourceOffers(SchedulerDriver driver,
                               List<Offer> offers) {}

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {}

    @Override
    public void frameworkMessage(SchedulerDriver driver, SlaveID slaveId, ExecutorID executorId, byte[] data) {}

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

    @Override
    public void error(SchedulerDriver driver, int code, String message) {}
  }

  public static void main(String[] args) throws Exception {
    ExecutorInfo executorInfo;

    File file = new File("./test_executor");
    executorInfo = ExecutorInfo.newBuilder()
                     .setExecutorId(ExecutorID.newBuilder().setValue("default").build())
                     .setUri(file.getCanonicalPath())
                     .build();

    new MesosSchedulerDriver(new MyScheduler(), "Exception Framework", executorInfo, args[0]).run();
  }
}
