package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

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
    PCollection<KV<Integer, Integer>> graphOut = p
        .apply(new LoadGraph("jstor", options.getGraphFile(), options.getGraphDestFirst()));
    graphOut.apply("StringifyLinks", ParDo.of(new DoFn<KV<Integer, Integer>, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        c.output(c.element().getKey() + "\t" + c.element().getValue());
      }
    })).apply("SaveData", TextIO.Write.to(options.getGraphFile() + "_processed/graph-").withSuffix(".txt"));

    p.run();
  }
}
