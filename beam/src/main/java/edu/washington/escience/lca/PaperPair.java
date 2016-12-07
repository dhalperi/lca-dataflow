package edu.washington.escience.lca;

import com.google.common.base.MoreObjects;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class PaperPair {
  int p1;
  int p2;

  public static PaperPair of(int p1, int p2) {
    return new PaperPair(p1, p2);
  }

  /////////////////////////////////////////////////////////////////////////////
  public PaperPair() {}
  private PaperPair(int p1, int p2) {
    this.p1 = p1;
    this.p2 = p2;
  }
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PaperPair)) {
      return false;
    }
    PaperPair other = (PaperPair) o;
    return Objects.equals(p1, other.p1) && Objects.equals(p2, other.p2);
  }
  @Override
  public int hashCode() {
    return Objects.hash(p1, p2);
  }
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(PaperPair.class)
        .add("p1", p1)
        .add("p2", p2)
        .toString();
  }
}
