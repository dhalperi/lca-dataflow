package edu.washington.escience.lca;

import java.util.Set;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Verify;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.collect.Sets;

public class ReachableStep extends PTransform<PCollectionTuple, PCollectionTuple> {
	private static final long serialVersionUID = 1L;
	private final TupleTag<KV<Integer, Reachable>> reachableInTag;
	private final TupleTag<KV<Integer, Reachable>> deltaInTag;
	private final TupleTag<KV<Integer, Integer>> graphTag;
	private final TupleTag<KV<Integer, Reachable>> reachableOutTag;
	private final TupleTag<KV<Integer, Reachable>> deltaOutTag;
	private final int step;
	private final String outputDirectory;

	public ReachableStep(
			TupleTag<KV<Integer, Reachable>> reachableInTag,
			TupleTag<KV<Integer, Reachable>> deltaInTag,
			TupleTag<KV<Integer, Integer>> graphTag,
			TupleTag<KV<Integer, Reachable>> reachableOutTag,
			TupleTag<KV<Integer, Reachable>> deltaOutTag,
			int step,
			String outputDirectory) {
		this.reachableInTag = reachableInTag;
		this.deltaInTag = deltaInTag;
		this.graphTag = graphTag;
		this.reachableOutTag = reachableOutTag;
		this.deltaOutTag = deltaOutTag;
		this.step = step;
		this.outputDirectory = outputDirectory;
	}

	@Override
	public PCollectionTuple apply(PCollectionTuple input) {
		PCollection<KV<Integer, Reachable>> oldDelta = input.get(deltaInTag);
		PCollection<KV<Integer, Integer>> graphOut = input.get(graphTag);

		/* Join the oldDelta with the graph to get the new set of one-hop edges. */
		TupleTag<Reachable> keyedDeltaTag = TupleTagUtil.makeTag();
		TupleTag<Integer> keyedGraphTag = TupleTagUtil.makeTag();
		PCollection<KV<Integer, Reachable>> oneHop =
				KeyedPCollectionTuple.of(keyedDeltaTag, oldDelta)
				.and(keyedGraphTag, graphOut)
				.apply("JoinGraphWithDelta_"+step, CoGroupByKey.<Integer>create())
				.apply("ComputeOneHop_"+step, ParDo.of(new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Reachable>>(){
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(ProcessContext c) throws Exception {
						KV<Integer, CoGbkResult> result = c.element();
						// We do not need the key - the vertex on which we have joined.
						// Instead, we just output the the sources (input of edges) can reach the
						// newly discovered destinations.
						CoGbkResult join = result.getValue();
						Iterable<Reachable> delta = join.getAll(keyedDeltaTag);
						Iterable<Integer> sources = join.getAll(keyedGraphTag);
						for (Reachable dest : delta) {
							Reachable newDest = Reachable.of(dest.dst, dest.depth+1);
							for (Integer src : sources) {
								c.output(KV.of(src, newDest));
							}
						}
					}}));

		/* Join the oneHop edges with the old reachable to produce the new reachable and the new delta in one step. */
		TupleTag<Reachable> keyedReachableTag = new TupleTag<Reachable>(){};
		TupleTag<Reachable> keyedJoinResultTag = new TupleTag<Reachable>(){};
		PCollection<KV<Integer, Reachable>> oldReachable = input.get(reachableInTag);

		PCollectionTuple output = KeyedPCollectionTuple.of(keyedReachableTag, oldReachable)
				.and(keyedJoinResultTag, oneHop)
				.apply("CoGroupReachableAndDelta_"+step, CoGroupByKey.<Integer>create())
				.apply("UpdateReachableAndDelta_"+step, ParDo
						.withOutputTags(reachableOutTag, TupleTagList.of(deltaOutTag))
						.of(new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Reachable>>(){
							private static final long serialVersionUID = 1L;

							@Override
							public void processElement(ProcessContext c) throws Exception {
								KV<Integer, CoGbkResult> result = c.element();
								Integer source = result.getKey();
								CoGbkResult join = result.getValue();
								Set<Integer> alreadyKnown = Sets.newHashSet();
								Set<Reachable> newReachable = Sets.newHashSet(join.getAll(keyedReachableTag));
								Set<Reachable> newDelta = Sets.newHashSet();
								// Record all the destinations this src can already.
								for (Reachable r : newReachable) {
									Verify.verify(alreadyKnown.add(r.dst), "Adding already known destination %s to src %s", r.dst, source);
								}
								for (Reachable r : join.getAll(keyedJoinResultTag)) {
									if (alreadyKnown.add(r.dst)) {
										newReachable.add(r);
										newDelta.add(r);
									}
								}
								// Emit all the new reachable set, which includes the new results.
								for (Reachable r : newReachable) {
									c.output(KV.of(source, r));
								}
								// Emit all the new delta set, which has been uniquified and deltaed.
								for (Reachable d : newDelta) {
									c.sideOutput(deltaOutTag, KV.of(source, d));
								}
							}}));

		//		output.get(reachableOutTag)
		//		.apply("StringifyReachable_"+step, ParDo.of(new StringifyReachable()))
		//		.apply("OutputReachable_"+step, TextIO.Write.to(outputDirectory + "/reachable" + step).withSuffix(".txt"));
		//
		//		output.get(deltaOutTag)
		//		.apply("StringifyDelta_"+step, ParDo.of(new StringifyReachable()))
		//		.apply("OutputDelta_"+step, TextIO.Write.to(outputDirectory + "/delta" + step).withSuffix(".txt"));

		return output;
	}
}
