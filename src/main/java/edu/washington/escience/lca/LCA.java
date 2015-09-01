package edu.washington.escience.lca;

import java.util.List;
import java.util.Set;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.KvSwap;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Sets;

public class LCA {

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

		@Description("Directory to place output files.")
		@Required
		String getOutputDirectory();
		void setOutputDirectory(String outputDir);
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		PCollection<KV<Integer, Integer>> graphOut = p.apply(new LoadGraph("jstor", options.getGraphFile(), options.getGraphDestFirst()));
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

		//		reachable0
		//		.apply("StringifyReachable_0", ParDo.of(new StringifyReachable()))
		//		.apply("OutputReachable_0", TextIO.Write.to(options.getOutputDirectory() + "/reachable0").withSuffix(".txt"));

		//		delta0
		//		.apply("StringifyDelta_0", ParDo.of(new StringifyReachable()))
		//		.apply("OutputDelta_0", TextIO.Write.to(options.getOutputDirectory() + "/delta0").withSuffix(".txt"));

		PCollection<KV<Integer, Reachable>> reachable = reachable0;
		PCollection<KV<Integer, Reachable>> delta = delta0;

		for (int i = 1; i < 8; ++i) {
			TupleTag<KV<Integer, Reachable>> reachableInTag = new TupleTag<KV<Integer, Reachable>>(){};
			TupleTag<KV<Integer, Reachable>> reachableOutTag = new TupleTag<KV<Integer, Reachable>>(){};
			TupleTag<KV<Integer, Integer>> graphTag = new TupleTag<KV<Integer, Integer>>(){};
			TupleTag<KV<Integer, Reachable>> deltaInTag = new TupleTag<KV<Integer, Reachable>>(){};
			TupleTag<KV<Integer, Reachable>> deltaOutTag = new TupleTag<KV<Integer, Reachable>>(){};
			ReachableStep step = new ReachableStep(reachableInTag, deltaInTag, graphTag, reachableOutTag, deltaOutTag,
					i, options.getOutputDirectory());

			PCollectionTuple oneHopResults = step.apply(PCollectionTuple.of(graphTag, graphIn)
					.and(reachableInTag, reachable)
					.and(deltaInTag, delta));

			reachable = oneHopResults.get(reachableOutTag);
			delta = oneHopResults.get(deltaOutTag);
		}

		reachable
		.apply("StringifyReachable", ParDo.of(new StringifyReachable()))
		.apply("OutputReachable", TextIO.Write.to(options.getOutputDirectory() + "/reachable").withSuffix(".txt"));

		p.run();
	}
}
