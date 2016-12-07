package edu.washington.escience.lca;

import static com.google.common.base.Verify.verify;

import com.google.common.collect.Sets;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Set;

@SuppressWarnings("serial")
public class ReachableStep extends PTransform<PCollectionTuple, PCollectionTuple> {
  private static final long serialVersionUID = 1L;
  private final TupleTag<KV<Integer, Reachable>> reachableInTag;
  private final TupleTag<KV<Integer, Reachable>> deltaInTag;
  private final TupleTag<KV<Integer, Integer>> graphTag;
  private final TupleTag<KV<Integer, Reachable>> reachableOutTag;
  private final TupleTag<KV<Integer, Reachable>> deltaOutTag;
  private final int step;
  private final String outputDirectory;
  private final boolean debug;

  public ReachableStep(
      TupleTag<KV<Integer, Reachable>> reachableInTag,
      TupleTag<KV<Integer, Reachable>> deltaInTag,
      TupleTag<KV<Integer, Integer>> graphTag,
      TupleTag<KV<Integer, Reachable>> reachableOutTag,
      TupleTag<KV<Integer, Reachable>> deltaOutTag,
      int step,
      String outputDirectory,
      boolean debug) {
    this.reachableInTag = reachableInTag;
    this.deltaInTag = deltaInTag;
    this.graphTag = graphTag;
    this.reachableOutTag = reachableOutTag;
    this.deltaOutTag = deltaOutTag;
    this.step = step;
    this.outputDirectory = outputDirectory;
    this.debug = debug;
  }

  @Override
  public PCollectionTuple apply(PCollectionTuple input) {
    PCollection<KV<Integer, Reachable>> oldDelta = input.get(deltaInTag);
    PCollection<KV<Integer, Integer>> graphOut = input.get(graphTag);

    /* Join the oldDelta with the graph to get the new set of one-hop edges. */
    TupleTag<Reachable> keyedDeltaTag = new TupleTag<Reachable>(){};
    TupleTag<Integer> keyedGraphTag = new TupleTag<Integer>(){};
    PCollection<KV<Integer, Reachable>> oneHop =
        KeyedPCollectionTuple.of(keyedDeltaTag, oldDelta)
        .and(keyedGraphTag, graphOut)
        .apply("JoinGraphWithDelta_"+step, CoGroupByKey.<Integer>create())
        .apply("ComputeOneHop_"+step, ParDo.of(new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Reachable>>(){
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
                  verify(alreadyKnown.add(r.dst), "Adding already known destination %s to src %s", r.dst, source);
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
      output.get(deltaOutTag)
      .apply("StringifyDelta_" + step, ParDo.of(new StringifyReachable()))
      .apply("OutputDelta_" + step, TextIO.Write.to(outputDirectory + "/delta" + step).withSuffix(".txt"));
    }

    return output;
  }
}
