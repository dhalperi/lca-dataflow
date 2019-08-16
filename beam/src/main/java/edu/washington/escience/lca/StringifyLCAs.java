package edu.washington.escience.lca;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

@SuppressWarnings("serial")
public class StringifyLCAs extends DoFn<KV<PaperPair, Ancestor>, String> {
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    KV<PaperPair, Ancestor> element = c.element();
    PaperPair pair = element.getKey();
    Ancestor ancestor = element.getValue();
    c.output(
        String.format(
            "%d\t%d\t%d\t%d\t%d\t%d",
            pair.p1, pair.p2, ancestor.id, ancestor.year, ancestor.d1, ancestor.d2));
  }
}
