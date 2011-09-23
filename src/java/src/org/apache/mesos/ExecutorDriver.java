package org.apache.mesos;

import org.apache.mesos.Protos.*;


/**
 * Abstract interface for driving an executor connected to Mesos.
 * This interface is used both to start the executor running (and
 * communicating with the slave) and to send information from the executor
 * to Nexus (such as status updates). Concrete implementations of
 * ExecutorDriver will take a Executor as a parameter in order to make
 * callbacks into it on various events.
 */
public interface ExecutorDriver {
  // Lifecycle methods.
  public Status start();
  public Status stop();
  public Status stop(boolean failover);
  public Status abort();
  public Status join();
  public Status run();

  // Communication methods.
  public Status sendStatusUpdate(TaskStatus status);
  public Status sendFrameworkMessage(byte[] data);
}
