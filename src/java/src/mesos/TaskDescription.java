package mesos;

import java.util.Map;


public class TaskDescription {
  public TaskDescription(TaskID taskId, SlaveID slaveId, String name, Map<String, String> params, byte[] data) {
    this.taskId = taskId;
    this.slaveId = slaveId;
    this.name = name;
    this.params = params;
    this.data = data;
  }

  // TODO(benh): Implement equals, hashCode, and toString.

  public final TaskID taskId;
  public final SlaveID slaveId;
  public final String name;
  public final Map<String, String> params;
  public final byte[] data;
}
