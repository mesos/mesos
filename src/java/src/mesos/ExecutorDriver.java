package mesos;


/**
 * Abstract interface for driving an executor connected to Nexus.
 * This interface is used both to start the executor running (and
 * communicating with the slave) and to send information from the executor
 * to Nexus (such as status updates). Concrete implementations of
 * ExecutorDriver will take a Executor as a parameter in order to make
 * callbacks into it on various events.
 */
public abstract class ExecutorDriver {
  // Lifecycle methods.
  public abstract int start();
  public abstract int stop();
  public abstract int join();
  public int run() {
    int ret = start();
    return ret != 0 ? ret : join();
  }

  // Communication methods.
  public abstract int sendStatusUpdate(TaskStatus status);
  public abstract int sendFrameworkMessage(FrameworkMessage message);
}
