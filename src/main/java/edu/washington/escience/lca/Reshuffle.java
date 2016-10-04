package edu.washington.escience.lca;

import java.util.Random;

import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

@SuppressWarnings("serial")
public class Reshuffle<T> extends PTransform<PCollection<T>, PCollection<T>> {
  private static class AddArbitraryKey<T> extends DoFn<T, KV<Integer, T>> {
    private transient Random random;

    @Override
    public void startBundle(Context c) {
      random = new Random();
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(KV.of(random.nextInt(), c.element()));
    }
  }

  private static class RemoveArbitraryKey<T> extends DoFn<KV<Integer, Iterable<T>>, T> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      for (T s : c.element().getValue()) {
        c.output(s);
      }
    }
  }

  @Override
  public
  PCollection<T> apply(PCollection<T> input) {
    return input.apply(ParDo.of(new AddArbitraryKey<T>())).setCoder(KvCoder.of(VarIntCoder.of(), input.getCoder()))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new RemoveArbitraryKey<T>()));
  }
}
