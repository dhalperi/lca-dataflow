package edu.washington.escience.lca;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Verify;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.Keys;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("serial")
public class LCAStep extends PTransform<PCollectionTuple, PCollectionTuple> {
	private static final long serialVersionUID = 1L;
	private final TupleTag<KV<Integer, Reachable>> reachableInTag;
	private final TupleTag<KV<Integer, Reachable>> deltaInTag;
	private final TupleTag<KV<PaperPair, Ancestor>> ancestorsInTag;
	private final TupleTag<KV<Integer, Integer>> graphTag;
	private final TupleTag<KV<Integer, Reachable>> reachableOutTag;
	private final TupleTag<KV<Integer, Reachable>> deltaOutTag;
	private final TupleTag<KV<PaperPair, Ancestor>> ancestorsOutTag;
	private final PCollectionView<Map<Integer, Integer>> paperYears;
	private final int step;
	private final String outputDirectory;
	private final boolean debug;

	public LCAStep(
			TupleTag<KV<Integer, Reachable>> reachableInTag,
			TupleTag<KV<Integer, Reachable>> deltaInTag,
			TupleTag<KV<PaperPair, Ancestor>> ancestorsInTag,
			TupleTag<KV<Integer, Integer>> graphTag,
			TupleTag<KV<Integer, Reachable>> reachableOutTag,
			TupleTag<KV<Integer, Reachable>> deltaOutTag,
			TupleTag<KV<PaperPair, Ancestor>> ancestorsOutTag,
			PCollectionView<Map<Integer, Integer>> paperYears,
			int step,
			String outputDirectory,
			boolean debug) {
		this.reachableInTag = reachableInTag;
		this.deltaInTag = deltaInTag;
		this.ancestorsInTag = ancestorsInTag;
		this.graphTag = graphTag;
		this.reachableOutTag = reachableOutTag;
		this.deltaOutTag = deltaOutTag;
		this.ancestorsOutTag = ancestorsOutTag;
		this.paperYears = paperYears;
		this.step = step;
		this.outputDirectory = outputDirectory;
		this.debug = debug;
	}

	@Override
	public PCollectionTuple apply(PCollectionTuple input) {
		PCollection<KV<Integer, Reachable>> oldDelta = input.get(deltaInTag);
		PCollection<KV<Integer, Reachable>> oldReachable = input.get(reachableInTag);
		PCollection<KV<PaperPair, Ancestor>> oldAncestors = input.get(ancestorsInTag);

		/* Produce new potential ancestors from oldReachable x oldDelta */
		// Generate a list of all seed-pairs that already have an ancestor. We do not need
		// to look at the new ancestors, because they are necessarily of a deeper depth.
		PCollectionView<Set<PaperPair>> knownAncestors = oldAncestors
				.apply("AncestorKeys", Keys.create())
				.apply("AncestorKeySet", Combine.globally(new SetUnionFn<>()))
				.apply("KnownAncestors", View.asSingleton());
		/* Join the oldDelta with the graph to get the new set of one-hop edges. */
		TupleTag<Reachable> keyedDeltaTag = new TupleTag<Reachable>(){};
		TupleTag<Reachable> keyedReachableTag = new TupleTag<Reachable>(){};
		PCollection<KV<PaperPair, Ancestor>> newAncestors =
				KeyedPCollectionTuple.of(keyedDeltaTag, oldDelta)
				.and(keyedReachableTag, oldReachable)
				.apply("JoinDeltaWithReachable", CoGroupByKey.<Integer>create())
				.apply("ComputeNewAncestors", ParDo
						.withSideInputs(knownAncestors, paperYears)
						.of(new DoFn<KV<Integer, CoGbkResult>, KV<PaperPair, Ancestor>>(){
						  private List<Reachable> delta = null;
							@Override
							public void processElement(ProcessContext c) throws Exception {
								// Key is the ancestor -- both delta and reachable vertices can reach it.
								KV<Integer, CoGbkResult> result = c.element();
								int ancestor = result.getKey();
								CoGbkResult join = result.getValue();
								Set<PaperPair> alreadyFound = c.sideInput(knownAncestors);
								Map<Integer, Integer> ancestorYears = c.sideInput(paperYears);
                if (delta == null) {
                  delta = Lists.newArrayList(join.getAll(keyedDeltaTag));
                }
                for (Reachable r2 : join.getAll(keyedReachableTag)) {
								  for (Reachable r1 : delta) {
										if (r1.dst == r2.dst) {
											// Skip delta join delta, which happens since reachable already contains delta.
											continue;
										} else if (r2.dst < r1.dst) {
											// Swap r1, r2 so that r1 < r2.
											Reachable t = r1;
											r1 = r2;
											r2 = t;
										}
										PaperPair p = PaperPair.of(r1.dst, r2.dst);
										if (!alreadyFound.contains(p)) {
											c.output(KV.of(p,  Ancestor.of(ancestor, r1.depth, r2.depth, ancestorYears.get(ancestor))));
										}
									}
								}
							}
						}))
				.apply("NewLeastCommonAncestors", Min.perKey());
		PCollection<KV<PaperPair, Ancestor>> newLCAs =
				PCollectionList.of(oldAncestors).and(newAncestors)
				.apply("CombinedCommonAncestors", Flatten.<KV<PaperPair, Ancestor>>pCollections());

		/* Every few steps, insert an extra reshuffle to break too much flatten unzipping. */
		if (step % 5 == 1) {
			newLCAs = newLCAs
					.apply("LeastCommonAncestors", new Reshuffle<>());
		}

		PCollection<KV<Integer, Integer>> graphOut = input.get(graphTag);
		/* Join the oldDelta with the graph to get the new set of one-hop edges. */
		TupleTag<Integer> keyedGraphTag = new TupleTag<Integer>(){};
		PCollection<KV<Integer, Reachable>> oneHop =
				KeyedPCollectionTuple.of(keyedDeltaTag, oldDelta)
				.and(keyedGraphTag, graphOut)
				.apply("JoinGraphWithDelta", CoGroupByKey.<Integer>create())
				.apply("ComputeOneHop", ParDo.of(new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Reachable>>(){
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
		TupleTag<Reachable> keyedJoinResultTag = new TupleTag<Reachable>(){};

		PCollectionTuple newDeltaAndReachable =
				KeyedPCollectionTuple.of(keyedReachableTag, oldReachable)
				.and(keyedJoinResultTag, oneHop)
				.apply("CoGroupReachableAndDelta", CoGroupByKey.<Integer>create())
				.apply("UpdateReachableAndDelta", ParDo
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

		if (debug) {
			newDeltaAndReachable.get(deltaOutTag)
			.apply("StringifyDelta", ParDo.of(new StringifyReachable()))
			.apply("OutputDelta", TextIO.Write.to(outputDirectory + "/delta" + step).withSuffix(".txt"));
		}

		return newDeltaAndReachable.and(ancestorsOutTag, newLCAs);
	}
}
