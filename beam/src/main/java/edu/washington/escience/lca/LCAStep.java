package edu.washington.escience.lca;

import static com.google.common.base.Verify.verify;

import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

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
  public PCollectionTuple expand(PCollectionTuple input) {
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
              private Aggregator<Integer, Integer> emptyDeltas =
                  createAggregator("empty deltas", new Sum.SumIntegerFn());
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                // Key is the ancestor -- both delta and reachable vertices can reach it.
                KV<Integer, CoGbkResult> result = c.element();
                int ancestor = result.getKey();
                CoGbkResult join = result.getValue();
                Set<PaperPair> alreadyFound = c.sideInput(knownAncestors);
                Map<Integer, Integer> ancestorYears = c.sideInput(paperYears);
                List<Reachable> delta = Lists.newArrayList(join.getAll(keyedDeltaTag));
                if (delta.isEmpty()) {
                  emptyDeltas.addValue(1);
                  return;
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
        .apply("CombinedCommonAncestors", Flatten.pCollections());

    /* Insert an extra reshuffle to break too much flatten unzipping. */
    if (step % 4 == 2) {
      newLCAs = newLCAs.apply("LeastCommonAncestors", new Reshuffle<>());
    }

    PCollection<KV<Integer, Integer>> graphOut = input.get(graphTag);
    /* Join the oldDelta with the graph to get the new set of one-hop edges. */
    TupleTag<Integer> keyedGraphTag = new TupleTag<Integer>(){};
    PCollection<KV<Integer, Reachable>> oneHop =
        KeyedPCollectionTuple.of(keyedDeltaTag, oldDelta)
        .and(keyedGraphTag, graphOut)
        .apply("JoinGraphWithDelta", CoGroupByKey.create())
        .apply("ComputeOneHop", ParDo.of(new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Reachable>>(){
          @ProcessElement
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
        .apply("CoGroupReachableAndDelta", CoGroupByKey.create())
        .apply("UpdateReachableAndDelta", ParDo
            .withOutputTags(reachableOutTag, TupleTagList.of(deltaOutTag))
            .of(new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Reachable>>(){
              private static final long serialVersionUID = 1L;
              @ProcessElement
              public void processElement(ProcessContext c) throws Exception {
                KV<Integer, CoGbkResult> result = c.element();
                Integer source = result.getKey();
                CoGbkResult join = result.getValue();
                Set<Integer> alreadyKnown = Sets.newHashSet();
                Set<Reachable> newReachable = Sets.newHashSet(join.getAll(keyedReachableTag));
                Set<Reachable> newDelta = Sets.newHashSet();
                // Record all the destinations this src can already.
                for (Reachable r : newReachable) {
                  verify(alreadyKnown.add(r.dst),
                      "Adding already known destination %s to src %s", r.dst, source);
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
