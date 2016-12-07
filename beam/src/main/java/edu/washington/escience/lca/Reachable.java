package edu.washington.escience.lca;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class Reachable {
  final int dst;
  final int depth;

  public Reachable(int dst, int depth) {
    this.dst = dst;
    this.depth = depth;
  }

  public static Reachable of(int dst, int depth) {
    return new Reachable(dst, depth);
  }

  @SuppressWarnings("unused") // used by AvroCoder
  private Reachable() {
    this(-1, -1);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dst, depth);
  }

  @Override
  public boolean equals (Object other) {
    if (!(other instanceof Reachable)) {
      return false;
    }
    Reachable o = (Reachable) other;
    return o.dst == dst && o.depth == depth;
  }
}
