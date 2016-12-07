package edu.washington.escience.lca;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

@SuppressWarnings("serial")
public class LoadSeeds extends PTransform<PInput, PCollection<Set<Integer>>> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadSeeds.class);
  private final String name;
  private final String path;

  public LoadSeeds(String name, String path) {
    this.name = name;
    this.path = path;
  }

  public static class ExtractSeedDoFn extends DoFn<String, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
        c.output(Integer.parseInt(c.element()));
      } catch (NumberFormatException e) {
        LOG.warn("Error extracting seed {}", c.element());
      }
    }
  }

  @Override
  public PCollection<Set<Integer>> apply(PInput input) {
    return input.getPipeline()
        .apply("Read_" + name, TextIO.Read.from(path))
        .apply("ConvertToInts_" + name, ParDo.of(new ExtractSeedDoFn()))
        .apply("Unify_" + name, Combine.globally(new SetUnionFn<Integer>()));
  }
}
