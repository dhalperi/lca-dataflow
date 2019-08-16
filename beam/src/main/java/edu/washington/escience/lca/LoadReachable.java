package edu.washington.escience.lca;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

@SuppressWarnings("serial")
public class LoadReachable extends PTransform<PInput, PCollection<KV<Integer, Reachable>>> {
  private final String name;
  private final String path;

  public LoadReachable(String name, String path) {
    this.name = name;
    this.path = path;
  }

  @Override
  public PCollection<KV<Integer, Reachable>> expand(PInput input) {
    return input
        .getPipeline()
        .apply("Read_" + name, TextIO.read().from(path))
        .apply("ConvertToReachables_" + name, ParDo.of(new ExtractReachableDoFn()));
  }

  public static class ExtractReachableDoFn extends DoFn<String, KV<Integer, Reachable>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] split = c.element().split("\\s+", 3);
      if (split.length != 3) {
        return;
      }
      try {
        c.output(
            KV.of(
                Integer.parseInt(split[0]),
                Reachable.of(Integer.parseInt(split[1]), Integer.parseInt(split[2]))));
      } catch (NumberFormatException e) {
        /* Pass */
      }
    }
  }
}
