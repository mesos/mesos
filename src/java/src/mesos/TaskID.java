package mesos;


public class TaskID {
  public TaskID(int i) {
    this.i = i;
  }

  public boolean equals(Object that) {
    if (!(that instanceof TaskID))
      return false;

    return i == ((TaskID) that).i;
  }

  public int hashCode() {
    return i;
  }

  public String toString() {
    return Integer.toString(i);
  }

  public final int i;
}