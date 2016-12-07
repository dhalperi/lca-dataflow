package edu.washington.escience.lca;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class LoadPapers extends PTransform<PInput, PCollection<Map<Integer, Integer>>> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadPapers.class);
  private final String name;
  private final String path;

  public LoadPapers(String name, String path) {
    this.name = name;
    this.path = path;
  }

  public static class ExtractPaper extends DoFn<String, KV<Integer, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String line = c.element();
      String[] split = line.split(",", 2);
      if (split.length != 2) {
        LOG.warn("LoadPapers: Skipping line {}", line);
      }
      try {
        c.output(KV.of(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
      } catch (NumberFormatException e) {
        LOG.warn("LoadPapers: Skipping line {}", line);
      }
    }
  }

  public static class MapUnionFn extends
      CombineFn<KV<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Integer>> {
    @Override
    public Map<Integer, Integer> createAccumulator() { return new HashMap<>(); }
    @Override
    public Map<Integer, Integer> addInput(Map<Integer, Integer> accum, KV<Integer, Integer> input) {
      accum.put(input.getKey(), input.getValue());
      return accum;
    }
    @Override
    public Map<Integer, Integer> mergeAccumulators(Iterable<Map<Integer, Integer>> accums) {
      Map<Integer, Integer> merged = createAccumulator();
      for (Map<Integer, Integer> accum : accums) {
        merged.putAll(accum);
      }
      return merged;
    }
    @Override
    public Map<Integer, Integer> extractOutput(Map<Integer, Integer> accum) {
      return accum;
    }
  }

  @Override
  public PCollection<Map<Integer, Integer>> apply(PInput input) {
    return input.getPipeline()
        .apply("Read_" + name, TextIO.Read.from(path))
        .apply(name, ParDo.of(new ExtractPaper()))
        .apply("Unify_" + name, Combine.globally(new MapUnionFn()));
  }

  interface Options extends PipelineOptions {
    @Description("File containing a list of papers with years and titles")
    @Required
    String getPapersFile();
    void setPapersFile(String file);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply(new LoadPapers("jstor_papers", options.getPapersFile()));
    p.run();
  }
}
