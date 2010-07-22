package mesos;


/**
 * Arguments passed to executors on initialization.
 */
public class ExecutorArgs {
  public ExecutorArgs(SlaveID slaveId, String host, FrameworkID frameworkId, String frameworkName, byte[] data) {
    this.slaveId = slaveId;
    this.host = host;
    this.frameworkId = frameworkId;
    this.frameworkName = frameworkName;
    this.data = data;
  }

  // TODO(benh): Implement equals, hashCode, and toString.

  public final SlaveID slaveId;
  public final String host;
  public final FrameworkID frameworkId;
  public final String frameworkName;
  public final byte[] data;
};
