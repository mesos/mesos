import java.io.File;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import mesos.*;

public class TestFramework {


  static class MyScheduler extends Scheduler {
    int launchedTasks = 0;
    int finishedTasks = 0;
    final int totalTasks = 5;

    @Override
    public String getFrameworkName(SchedulerDriver d) {
      return "Java test framework";
    }

    @Override
    public ExecutorInfo getExecutorInfo(SchedulerDriver d) {
      try {
        return new ExecutorInfo(
            new File("../../cpp-test-executor").getCanonicalPath(),
            new byte[0]);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
        return null;
      }
    }

    @Override
    public void registered(SchedulerDriver d, FrameworkID fid) {
      System.out.println("Registered! FID = " + fid);
    }

    @Override
    public void resourceOffer(SchedulerDriver d,
                              OfferID oid,
                              Collection<SlaveOffer> offers)
    {
      System.out.println("Got offer offer " + oid);

      ArrayList<TaskDescription> tasks = new ArrayList<TaskDescription>();

      for (SlaveOffer offer : offers) {
        if (launchedTasks < totalTasks) {
          TaskID taskId = new TaskID(launchedTasks++);
          Map<String, String> taskParams = new HashMap<String, String>();
          taskParams.put("cpus", "1");
          taskParams.put("mem", "134217728");
          System.out.println("Launching task " + taskId);
          tasks.add(new TaskDescription(taskId,
                                        offer.slaveId,
                                        "task " + taskId,
                                        taskParams,
                                        new byte[0]));
        }
      }
      Map<String, String> params = new HashMap<String, String>();
      params.put("timeout", "1");
      d.replyToOffer(oid, tasks, params);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID oid) {
      System.out.println("Offer " + oid + " was rescinded.");
    }

    @Override
    public void statusUpdate(SchedulerDriver d, TaskStatus status) {
      System.out.println("Status update: task " + status.taskId +
                         " is in state " + status.state);
      if (status.state == TaskState.TASK_FINISHED) {
        finishedTasks++;
        System.out.println("Finished tasks: " + finishedTasks);
        if (finishedTasks == totalTasks)
          d.stop();
      }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 FrameworkMessage message)
    {
      System.out.println("Received message: " + message);
    }
 
    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID sid) {
      System.out.println("Received notification that slave " + sid 
                       + "was lost.");
    }

    @Override
    public void error(SchedulerDriver d, int code, String message) {
      System.out.println("Error: " + message);
    }
  }

  public static void main(String[] args) throws Exception {
    new MesosSchedulerDriver(new MyScheduler(), args[0]).run();
    System.out.println("all done");
    System.gc();
  }
}
