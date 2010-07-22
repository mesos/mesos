package mesos;


/**
 * Callback interface to be implemented by frameworks' executors.
 */
public abstract class Executor {
  public abstract void init(ExecutorDriver driver, ExecutorArgs args);
  public abstract void launchTask(ExecutorDriver driver, TaskDescription task);
  public abstract void killTask(ExecutorDriver driver, TaskID taskId);
  public abstract void frameworkMessage(ExecutorDriver driver, FrameworkMessage message);
  public abstract void shutdown(ExecutorDriver driver);
  public abstract void error(ExecutorDriver driver, int code, String message);
}
