package mesos;


public enum TaskState {
  TASK_STARTING,
  TASK_RUNNING,
  TASK_FINISHED,
  TASK_FAILED,
  TASK_KILLED,
  TASK_LOST,
}