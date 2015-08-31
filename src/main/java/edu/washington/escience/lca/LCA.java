package edu.washington.escience.lca;

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
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class LCA {

	interface Options extends PipelineOptions {
		@Description("Input file containing a graph as a list of long-long pairs")
		@Required
		String getInputFile();
		void setInputFile(String file);

		@Description("True if the links are listed dest first.")
		@Default.Boolean(false)
		Boolean getInputDestFirst();
		void setInputDestFirst(Boolean destFirst);
	}

	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline p = Pipeline.create(options);

		PCollection<KV<Integer, Integer>> graphOut = p.apply(new LoadGraph("jstor", options.getInputFile(), options.getInputDestFirst()));
		PCollection<KV<Integer, Integer>> graphIn = graphOut.apply(KvSwap.<Integer, Integer>create());

		PCollection<KV<Integer, Reachable>> reachable0 = graphOut.apply("Reachable0", ParDo.of(
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

		reachable0
		.apply("StringifyReachable_0", ParDo.of(new StringifyReachable()))
		.apply("OutputReachable_0", TextIO.Write.to("output/reachable0.txt"));

		delta0
		.apply("StringifyDelta_0", ParDo.of(new StringifyReachable()))
		.apply("OutputDelta_0", TextIO.Write.to("output/delta0.txt"));

		PCollection<KV<Integer, Reachable>> reachable = reachable0;
		PCollection<KV<Integer, Reachable>> delta = delta0;

		for (int i = 1; i < 2; ++i) {
			TupleTag<KV<Integer, Reachable>> reachableInTag = TupleTagUtil.makeTag();
			TupleTag<KV<Integer, Reachable>> reachableOutTag = TupleTagUtil.makeTag();
			TupleTag<KV<Integer, Integer>> graphTag = TupleTagUtil.makeTag();
			TupleTag<KV<Integer, Reachable>> deltaInTag = TupleTagUtil.makeTag();
			TupleTag<KV<Integer, Reachable>> deltaOutTag = TupleTagUtil.makeTag();
			ReachableStep step = new ReachableStep(reachableInTag, deltaInTag, graphTag, reachableOutTag, deltaOutTag, i);

			PCollectionTuple oneHopResults = step.apply(PCollectionTuple.of(graphTag, graphIn)
					.and(reachableInTag, reachable)
					.and(deltaInTag, delta));

			reachable = oneHopResults.get(reachableOutTag);
			delta = oneHopResults.get(deltaOutTag);
		}
		p.run();
	}
}
