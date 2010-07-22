package mesos;


/**
 * Concrete implementation of ExecutorDriver that communicates with a
 * Nexus slave. The slave's location is read from environment variables
 * set by it when it execs the user's executor script; users only need
 * to create the NexusExecutorDriver and call run() on it.
 */
public class MesosExecutorDriver extends ExecutorDriver {
  static {
    System.loadLibrary("mesos");
  }

  public MesosExecutorDriver(Executor exec) {
    this.exec = exec;
  }

  protected native void initialize();
  protected native void finalize();

  // Lifecycle methods.
  public native int start();
  public native int stop();
  public native int join();

  public native int sendStatusUpdate(TaskStatus status);
  public native int sendFrameworkMessage(FrameworkMessage message);

  private Executor exec;
  
  private long __exec;
  private long __driver;
}
