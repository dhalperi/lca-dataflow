package edu.washington.escience.lca;

import com.google.common.collect.ComparisonChain;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Ancestor implements Comparable<Ancestor> {
  int id;
  int d1;
  int d2;
  int year;

  public static Ancestor of(int id, int d1, int d2, int year) {
    return new Ancestor(id, d1, d2, year);
  }

  public int getDepth() {
    return Math.max(d1, d2);
  }

  /////////////////////////////////////////////////////////////////////////////
  public Ancestor() {};

  private Ancestor(int id, int d1, int d2, int year) {
    this.id = id;
    this.d1 = d1;
    this.d2 = d2;
    this.year = year;
  }

  @Override
  public int compareTo(Ancestor o) {
    return ComparisonChain.start()
        .compare(this.getDepth(), o.getDepth()) // smaller max depth
        .compare(this.d1 + this.d2, o.d1 + o.d2) // smaller total depth
        .compare(o.year, this.year) // larger year
        .compare(this.id, o.id) // smaller id
        .result();
  }
}
