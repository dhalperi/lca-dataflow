package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

@SuppressWarnings("serial")
public class LoadReachable extends PTransform<PInput, PCollection<KV<Integer, Reachable>>> {
  private final String name;
  private final String path;

  public LoadReachable(String name, String path) {
    this.name = name;
    this.path = path;
  }

  @Override
  public PCollection<KV<Integer, Reachable>> apply(PInput input) {
    return input.getPipeline()
        .apply(TextIO.Read.named("Read_" + name).from(path))
        .apply("ConvertToReachables_" + name, ParDo.of(new ExtractReachableDoFn()));
  }

  public static class ExtractReachableDoFn extends DoFn<String, KV<Integer, Reachable>> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      String[] split = c.element().split("\\s+", 3);
      if (split.length != 3) {
        return;
      }
      try {
        c.output(KV.of(Integer.parseInt(split[0]), Reachable.of(Integer.parseInt(split[1]), Integer.parseInt(split[2]))));
      } catch (NumberFormatException e) {
        return;
      }
    }
  }
}
