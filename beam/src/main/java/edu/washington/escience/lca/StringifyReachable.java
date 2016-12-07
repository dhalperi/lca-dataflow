package edu.washington.escience.lca;

import com.google.common.base.Joiner;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class StringifyReachable extends DoFn<KV<Integer, Reachable>, String> {
  private static final long serialVersionUID = 1L;

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    KV<Integer, Reachable> element = c.element();
    Integer src = element.getKey();
    Reachable r = element.getValue();
    c.output(Joiner.on("\t").join(src, r.dst, r.depth));
  }

}
