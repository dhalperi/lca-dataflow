package edu.washington.escience.lca;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

@SuppressWarnings("serial")
public class CommonAncestors extends
    PTransform<PCollection<KV<Integer, Reachable>>, PCollection<KV<PaperPair, Ancestor>>> {
  private PCollectionView<Map<Integer, Integer>> papers;

  public CommonAncestors(PCollectionView<Map<Integer, Integer>> papers) {
    this.papers = papers;
  }

  private static class GenerateAncestors extends
      DoFn<KV<Integer, Iterable<Reachable>>, KV<PaperPair, Ancestor>> {
    private PCollectionView<Map<Integer, Integer>> papers;

    public GenerateAncestors(PCollectionView<Map<Integer, Integer>> papers) {
      this.papers = papers;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      KV<Integer, Iterable<Reachable>> element = c.element();
      int ancestor = element.getKey();
      int year = c.sideInput(papers).get(ancestor);
      Iterable<Reachable> papers = element.getValue();
      for (Reachable p1 : papers) {
        for (Reachable p2 : papers) {
          if (p1.dst < p2.dst) {
            c.output(KV.of(PaperPair.of(p1.dst, p2.dst), Ancestor.of(ancestor, p1.depth, p2.depth, year)));
          }
        }
      }
    }
  }

  @Override
  public PCollection<KV<PaperPair, Ancestor>> expand(PCollection<KV<Integer, Reachable>> input) {
    return input
        .apply("GroupByAncestor", GroupByKey.create())
        .apply("GeneratePairs", ParDo.withSideInputs(papers).of(new GenerateAncestors(papers)))
        .apply("LeastCommonAncestor", Combine.perKey(new AncestorCombineFn()));
  }

  public static class AncestorCombineFn extends BinaryCombineFn<Ancestor> {
    @Override
    public Ancestor identity() {
      return Ancestor.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE);
    }

    @Override
    public Ancestor apply(Ancestor left, Ancestor right) {
      // First factor: smaller max depth
      int depth_cmp = left.getDepth() - right.getDepth();
      if (depth_cmp < 0) {
        return left;
      } else if (depth_cmp > 0)  {
        return right;
      }
      // Second factor: smaller min depth
      depth_cmp = Math.min(left.d1, left.d2) - Math.min(right.d1, right.d2);
      if (depth_cmp < 0) {
        return left;
      } else if (depth_cmp > 0)  {
        return right;
      }
      // Third factor: larger year
      depth_cmp = left.year - right.year;
      if (depth_cmp > 0) {
        return left;
      } else if (depth_cmp < 0)  {
        return right;
      }
      // Fourth factor: arbitrary tiebreaker on paper id
      depth_cmp = left.id - right.id;
      if (depth_cmp < 0) {
        return left;
      } else if (depth_cmp > 0)  {
        return right;
      } else {
        throw new IllegalStateException("Expected to lose a tie by now. " + right + " " + left);
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  private interface Options extends PipelineOptions {
    @Description("File containing a list of papers with years and titles")
    @Required
    String getPapersFile();
    void setPapersFile(String file);

    @Description("File/Glob containing reachability results as long-long-int triples")
    @Required
    String getReachableFile();
    void setReachableFile(String path);

    @Description("Directory to place output files.")
    @Required
    String getOutputDirectory();
    void setOutputDirectory(String outputDir);
  }

  public static void main(String [] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    PCollectionView<Map<Integer, Integer>> papers = p
        .apply("LoadPapers", new LoadPapers("papers", options.getPapersFile()))
        .apply(View.asSingleton());

    PCollection<KV<Integer, Reachable>> reachable = p
        .apply("LoadReachable", new LoadReachable("reachable", options.getReachableFile()));

    reachable
    .apply(new CommonAncestors(papers))
    .apply("StringifyLCAs", ParDo.of(new StringifyLCAs()))
    .apply("OutputLCAs", TextIO.Write.to(options.getOutputDirectory() + "/lcas").withSuffix(".txt"));

    p.run();
  }
}
