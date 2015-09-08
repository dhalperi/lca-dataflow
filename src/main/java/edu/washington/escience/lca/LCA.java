package edu.washington.escience.lca;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.KvSwap;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
public class LCA {
	private static final Logger LOG = LoggerFactory.getLogger(LCA.class);

	interface Options extends PipelineOptions {
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
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);
		PCollectionView<Map<Integer, Integer>> papers = p
				.apply(new LoadPapers("papers", options.getPapersFile()))
				.apply(View.asSingleton());

		PCollection<KV<Integer, Integer>> graphOut = p
				.apply(new LoadGraph("jstor", options.getGraphFile(), options.getGraphDestFirst()))
				.apply("Filter_paper_years",
						ParDo.withSideInputs(papers)
						.of(new DoFn<KV<Integer, Integer>, KV<Integer, Integer>>() {
							@Override
							public void processElement(ProcessContext c) {
								Map<Integer, Integer> papersMap = c.sideInput(papers);
								KV<Integer, Integer> cite = c.element();
								Integer sourceYear = papersMap.get(cite.getKey());
								Integer dstYear = papersMap.get(cite.getValue());
								if (sourceYear == null || dstYear == null || sourceYear < dstYear) {
									LOG.warn("Dropping link {}({}) -> {}({})", cite.getKey(), sourceYear, cite.getValue(), dstYear);
									return;
								}
								c.output(cite);
							}
						}));
		PCollection<KV<Integer, Integer>> graphIn = graphOut.apply(KvSwap.<Integer, Integer>create());
		PCollection<Integer> seeds = p.apply(new LoadSeeds("seeds", options.getSeedsFile()));

		PCollectionView<List<Integer>> seedsView = seeds.apply(View.asList());

		PCollection<KV<Integer, Reachable>> reachable0 = graphOut
				.apply("FilterSeeds", ParDo.withSideInputs(seedsView).of(
						new DoFn<KV<Integer, Integer>, KV<Integer, Integer>>(){
							Set<Integer> seeds;
							@Override
							public void processElement(ProcessContext c) throws Exception {
								if (seeds == null) {
									seeds = Sets.newHashSet(c.sideInput(seedsView));
								}
								KV<Integer, Integer> link = c.element();
								if (seeds.contains(link.getKey())) {
									// The source of the link is a seed vertex
									c.output(link);
								}
							}
						}
						))
				.apply("Reachable_0", ParDo.of(
						new DoFn<KV<Integer, Integer>, KV<Integer, Reachable>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public void processElement(ProcessContext c)
									throws Exception {
								KV<Integer, Integer> link = c.element();
								c.output(KV.of(link.getKey(), Reachable.of(link.getValue(), 1)));
							}
						}));


		PCollection<KV<Integer, Reachable>> delta0 = reachable0;

		/* The setup for every loop: */
		PCollection<KV<Integer, Reachable>> delta = delta0;
		PCollectionList<KV<Integer, Reachable>> allDeltas = PCollectionList.of(delta);
		PCollection<KV<Integer, Reachable>> reachable = allDeltas.apply(Flatten.pCollections());

		for (int i = 1; i < 30; ++i) {
			TupleTag<KV<Integer, Reachable>> reachableInTag = new TupleTag<KV<Integer, Reachable>>(){};
			TupleTag<KV<Integer, Integer>> graphTag = new TupleTag<KV<Integer, Integer>>(){};
			TupleTag<KV<Integer, Reachable>> deltaInTag = new TupleTag<KV<Integer, Reachable>>(){};

			ReachableStep step = new ReachableStep(reachableInTag, deltaInTag, graphTag,
					i, options.getOutputDirectory(), false /* debug */);

			PCollection<KV<Integer,Reachable>> oneHopResults = step.apply(
					PCollectionTuple.of(graphTag, graphIn)
					.and(reachableInTag, reachable)
					.and(deltaInTag, delta));

			delta = oneHopResults;
			allDeltas = allDeltas.and(delta);
			reachable = allDeltas.apply(Flatten.pCollections());
		}

		reachable
		.apply("StringifyReachable", ParDo.of(new StringifyReachable()))
		.apply("OutputReachable", TextIO.Write.to(options.getOutputDirectory() + "/reachable").withSuffix(".txt"));

		p.run();
	}
}
