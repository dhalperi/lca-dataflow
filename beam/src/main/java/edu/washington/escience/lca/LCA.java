package edu.washington.escience.lca;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Runs the Least Common Ancestors algorithm for the specified citations dataset.
 */
@SuppressWarnings("serial")
public class LCA {
  private static final Logger LOG = LoggerFactory.getLogger(LCA.class);

  interface Options extends PipelineOptions {
    @Description("The number of iterations in the reachability computation")
    @Default.Integer(3)
    int getNumIterations();
    void setNumIterations(int numIterations);

    @Description("File containing a graph as a list of long-long pairs")
    @Required
    String getGraphFile();
    void setGraphFile(String file);

    @Description("True if the links are listed dest first.")
    @Default.Boolean(false)
    Boolean getGraphDestFirst();
    void setGraphDestFirst(Boolean destFirst);

    @Description("File containing a list of seeds as a list of longs")
    @Required
    String getSeedsFile();
    void setSeedsFile(String file);

    @Description("File containing a list of papers with years and titles")
    @Required
    String getPapersFile();
    void setPapersFile(String file);

    @Description("Directory to place output files.")
    @Required
    String getOutputDirectory();
    void setOutputDirectory(String outputDir);

    @Description("True if the job should be run in debug mode.")
    @Default.Boolean(false)
    Boolean getDebug();
    void setDebug(Boolean debug);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setOutputDirectory(options.getOutputDirectory() + "/lcas_" + options.getNumIterations());

    Pipeline p = Pipeline.create(options);
    PCollectionView<Map<Integer, Integer>> papers = p
        .apply(new LoadPapers("papers", options.getPapersFile()))
        .apply("papers", View.asSingleton());

    // graphOut is edges where src cites destination
    PCollection<KV<Integer, Integer>> graphOut = p
        .apply(new LoadGraph("jstor", options.getGraphFile(), options.getGraphDestFirst()))
        .apply("Filter_paper_years",
            ParDo.withSideInputs(papers)
            .of(new DoFn<KV<Integer, Integer>, KV<Integer, Integer>>() {
              private Aggregator<Integer, Integer> droppedPapers =
                  createAggregator("dropped papers", new Sum.SumIntegerFn());
              @ProcessElement
              public void processElement(ProcessContext c) {
                Map<Integer, Integer> papersMap = c.sideInput(papers);
                KV<Integer, Integer> cite = c.element();
                Integer sourceYear = papersMap.get(cite.getKey());
                Integer dstYear = papersMap.get(cite.getValue());
                if (sourceYear == null || dstYear == null || sourceYear < dstYear) {
                  droppedPapers.addValue(1);
                  LOG.debug("Dropping link {}({}) -> {}({})", cite.getKey(), sourceYear, cite.getValue(), dstYear);
                  return;
                }
                c.output(cite);
              }
            }));

    PCollectionView<Set<Integer>> seedsView = p
        .apply(new LoadSeeds("seeds", options.getSeedsFile()))
        .apply("seeds", View.asSingleton());

    PCollection<KV<Integer, Reachable>> reachable0 = graphOut
        .apply("FilterSeeds", ParDo.withSideInputs(seedsView).of(
            new DoFn<KV<Integer, Integer>, KV<Integer, Integer>>(){
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                KV<Integer, Integer> link = c.element();
                Set<Integer> seeds = c.sideInput(seedsView);
                if (seeds.contains(link.getKey())) {
                  // The source of the link is a seed vertex
                  c.output(link);
                }
              }
            }))
        .apply("Reachable_0", ParDo.of(
            new DoFn<KV<Integer, Integer>, KV<Integer, Reachable>>() {
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                KV<Integer, Integer> link = c.element();
                c.output(KV.of(link.getValue(), Reachable.of(link.getKey(), 1)));
              }
            }));

    PCollection<KV<Integer, Reachable>> delta0 = reachable0;

    // Begin the iteration process
    PCollection<KV<Integer, Reachable>> reachable = reachable0;
    PCollection<KV<Integer, Reachable>> delta = delta0;
    PCollection<KV<PaperPair, Ancestor>> ancestors = p
        .apply("Ancestors_0", Create.<KV<PaperPair, Ancestor>>of().withCoder(
            KvCoder.of(AvroCoder.of(PaperPair.class), AvroCoder.of(Ancestor.class))));

    for (int i = 1; i < options.getNumIterations(); ++i) {
      TupleTag<KV<Integer, Integer>> graphTag = new TupleTag<KV<Integer, Integer>>(){};
      TupleTag<KV<Integer, Reachable>> reachableInTag = new TupleTag<KV<Integer, Reachable>>(){};
      TupleTag<KV<Integer, Reachable>> reachableOutTag = new TupleTag<KV<Integer, Reachable>>(){};
      TupleTag<KV<Integer, Reachable>> deltaInTag = new TupleTag<KV<Integer, Reachable>>(){};
      TupleTag<KV<Integer, Reachable>> deltaOutTag = new TupleTag<KV<Integer, Reachable>>(){};
      TupleTag<KV<PaperPair, Ancestor>> ancestorsInTag = new TupleTag<KV<PaperPair, Ancestor>>(){};
      TupleTag<KV<PaperPair, Ancestor>> ancestorsOutTag = new TupleTag<KV<PaperPair, Ancestor>>(){};
      LCAStep step = new LCAStep(reachableInTag, deltaInTag, ancestorsInTag, graphTag,
          reachableOutTag, deltaOutTag, ancestorsOutTag, papers,
          i, options.getOutputDirectory(), options.getDebug());

      PCollectionTuple oneHopResults = PCollectionTuple
          .of(graphTag, graphOut)
          .and(reachableInTag, reachable)
          .and(deltaInTag, delta)
          .and(ancestorsInTag, ancestors)
          .apply("LCA_"+i, step);

      reachable = oneHopResults.get(reachableOutTag);
      delta = oneHopResults.get(deltaOutTag);
      ancestors = oneHopResults.get(ancestorsOutTag);
    }

    if (options.getDebug()) {
      reachable0
      .apply("StringifyReachable0", ParDo.of(new StringifyReachable()))
      .apply("OutputReachable0", TextIO.Write.to(options.getOutputDirectory() + "/reachable0").withSuffix(".txt"));

      reachable
      .apply("StringifyReachable", ParDo.of(new StringifyReachable()))
      .apply("OutputReachable", TextIO.Write.to(options.getOutputDirectory() + "/reachable").withSuffix(".txt"));
    }

    ancestors
    .apply("StringifyLCAs", ParDo.of(new StringifyLCAs()))
    .apply("OutputLCAs", TextIO.Write.to(options.getOutputDirectory() + "/lcas").withSuffix(".txt").withNumShards(1));

    p.run().waitUntilFinish();
  }
}
