package mesos;


public class FrameworkMessage {

  public FrameworkMessage(SlaveID slaveId, TaskID taskId, byte[] data) {
    this.slaveId = slaveId;
    this.taskId = taskId;
    this.data = data;
  }

  // TODO(benh): Implement equals, hashCode, and toString.

  public final SlaveID slaveId;
  public final TaskID taskId;
  public final byte[] data;
}
