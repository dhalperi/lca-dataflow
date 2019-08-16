package edu.washington.escience.lca;

import org.apache.beam.sdk.transforms.Combine.CombineFn;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@SuppressWarnings("serial")
public class SetUnionFn<T> extends CombineFn<T, Set<T>, Set<T>> {
  @Override
  public Set<T> createAccumulator() {
    return new HashSet<>();
  }

  @Override
  public Set<T> addInput(Set<T> accum, T input) {
    accum.add(input);
    return accum;
  }

  @Override
  public Set<T> mergeAccumulators(Iterable<Set<T>> accums) {
    Iterator<Set<T>> it = accums.iterator();
    if (!it.hasNext()) {
      return createAccumulator();
    }
    Set<T> merged = it.next();
    while (it.hasNext()) {
      merged.addAll(it.next());
    }
    return merged;
  }

  @Override
  public Set<T> extractOutput(Set<T> accum) {
    return accum;
  }
}
