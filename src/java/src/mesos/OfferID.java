package mesos;


public class OfferID {
  public OfferID(String s) {
    this.s = s;
  }

  public boolean equals(Object that) {
    if (!(that instanceof OfferID))
      return false;

    return s.equals(((OfferID) that).s);
  }

  public int hashCode() {
    return s.hashCode();
  }

  public String toString() {
    return s;
  }

  public final String s;
}