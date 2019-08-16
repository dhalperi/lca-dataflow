package edu.washington.escience.lca;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

@SuppressWarnings("serial")
public class LoadGraph extends PTransform<PInput, PCollection<KV<Integer, Integer>>> {
  private final String name;
  private final String path;
  private final boolean destFirst;

  public LoadGraph(String name, String path, boolean destFirst) {
    this.name = name;
    this.path = path;
    this.destFirst = destFirst;
  }

  public static class ExtractLinkDoFn extends DoFn<String, KV<Integer, Integer>> {
    private final boolean destFirst;

    public ExtractLinkDoFn(boolean destFirst) {
      this.destFirst = destFirst;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] fields = c.element().split(",", 2);
      if (fields.length != 2 || fields[0] == null || fields[1] == null) {
        return;
      }
      try {
        if (destFirst) {
          c.output(KV.of(Integer.parseInt(fields[1]), Integer.parseInt(fields[0])));
        } else {
          c.output(KV.of(Integer.parseInt(fields[0]), Integer.parseInt(fields[1])));
        }
      } catch (NumberFormatException e) {
        /* Pass */ ;
      }
    }
  }

  @Override
  public PCollection<KV<Integer, Integer>> expand(PInput input) {
    return input
        .getPipeline()
        .apply("Read" + name, TextIO.read().from(path))
        .apply("ConvertToInts", ParDo.of(new ExtractLinkDoFn(destFirst)));
  }
}
