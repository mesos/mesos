package mesos;


public class TaskStatus {

  public TaskStatus(TaskID taskId, TaskState state, byte[] data) {
    this.taskId = taskId;
    this.state = state;
    this.data = data;
  }

  // TODO(benh): Implement equals, hashCode, and toString.

  public final TaskID taskId;
  public final TaskState state;
  public final byte[] data;
}
