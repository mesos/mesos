package mesos;


public class FrameworkID {
  public static final FrameworkID EMPTY = new FrameworkID("");

  public FrameworkID(String s) {
    this.s = s;
  }

  public boolean equals(Object that) {
    if (!(that instanceof FrameworkID))
      return false;

    return s.equals(((FrameworkID) that).s);
  }

  public int hashCode() {
    return s.hashCode();
  }

  public String toString() {
    return s;
  }

  public final String s;
}