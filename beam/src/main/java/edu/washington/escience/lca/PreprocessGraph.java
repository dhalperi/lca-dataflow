package edu.washington.escience.lca;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@SuppressWarnings("serial")
public class PreprocessGraph {
  interface Options extends PipelineOptions {
    @Description("File containing a graph as a list of long-long pairs")
    @Required
    String getGraphFile();

    void setGraphFile(String file);

    @Description("True if the links are listed dest first.")
    @Default.Boolean(false)
    Boolean getGraphDestFirst();

    void setGraphDestFirst(Boolean destFirst);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    PCollection<KV<Integer, Integer>> graphOut =
        p.apply(new LoadGraph("jstor", options.getGraphFile(), options.getGraphDestFirst()));
    graphOut
        .apply(
            "StringifyLinks",
            ParDo.of(
                new DoFn<KV<Integer, Integer>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(c.element().getKey() + "\t" + c.element().getValue());
                  }
                }))
        .apply(
            "SaveData",
            TextIO.write().to(options.getGraphFile() + "_processed/graph-").withSuffix(".txt"));

    p.run();
  }
}
