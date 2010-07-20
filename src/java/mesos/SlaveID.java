package mesos;


public class SlaveID {

  public SlaveID(String s) {
    this.s = s;
  }

  public boolean equals(Object that) {
    if (!(that instanceof SlaveID))
      return false;

    return s.equals(((SlaveID) that).s);
  }

  public int hashCode() {
    return s.hashCode();
  }

  public String toString() {
    return s;
  }

  public final String s;
}