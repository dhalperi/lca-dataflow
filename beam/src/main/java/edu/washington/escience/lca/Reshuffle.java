package edu.washington.escience.lca;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Random;

@SuppressWarnings("serial")
public class Reshuffle<T> extends PTransform<PCollection<T>, PCollection<T>> {
  private static class AddArbitraryKey<T> extends DoFn<T, KV<Integer, T>> {
    private transient Random random;

    @Setup
    public void setup() {
      random = new Random();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(KV.of(random.nextInt(), c.element()));
    }
  }

  private static class RemoveArbitraryKey<T> extends DoFn<KV<Integer, Iterable<T>>, T> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      for (T s : c.element().getValue()) {
        c.output(s);
      }
    }
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(ParDo.of(new AddArbitraryKey<T>()))
        .setCoder(KvCoder.of(VarIntCoder.of(), input.getCoder()))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new RemoveArbitraryKey<T>()));
  }
}
