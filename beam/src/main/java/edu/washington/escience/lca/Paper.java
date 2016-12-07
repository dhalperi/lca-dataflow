package edu.washington.escience.lca;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Paper {
  Integer paper;
  @Nullable String title;
  Integer year;

  public Paper() {
    paper = -1;
    year = -1;
    title = null;
  }
}
