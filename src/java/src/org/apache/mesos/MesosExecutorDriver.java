package org.apache.mesos;

import org.apache.mesos.Protos.*;


/**
 * Concrete implementation of ExecutorDriver that communicates with a
 * Mesos slave. The slave's location is read from environment variables
 * set by it when it execs the user's executor script; users only need
 * to create the MesosExecutorDriver and call run() on it.
 */
public class MesosExecutorDriver implements ExecutorDriver {
  static {
    System.loadLibrary("mesos");
  }

  public MesosExecutorDriver(Executor exec) {
    this.exec = exec;

    initialize();
  }

  // Lifecycle methods.
  public native Status start();
  public Status stop() {
    return stop(false);
  }
  public native Status stop(boolean failover);
  public native Status abort();
  public native Status join();

  public Status run() {
    Status ret = start();
    return ret != Status.OK ? ret : join();
  }

  public native Status sendStatusUpdate(TaskStatus status);
  public native Status sendFrameworkMessage(byte[] data);

  protected native void initialize();
  protected native void finalize();

  private final Executor exec;

  private long __exec;
  private long __driver;
}

