package edu.washington.escience.lca;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

@SuppressWarnings("serial")
public class LoadSeeds extends PTransform<PInput, PCollection<Set<Integer>>> {
	private static final Logger LOG = LoggerFactory.getLogger(LoadSeeds.class);
	private final String name;
	private final String path;

	public LoadSeeds(String name, String path) {
		this.name = name;
		this.path = path;
	}

	public static class ExtractSeedDoFn extends DoFn<String, Integer> {
		@Override
		public void processElement(ProcessContext c) throws Exception {
			try {
				c.output(Integer.parseInt(c.element()));
			} catch (NumberFormatException e) {
				LOG.warn("Error extracting seed {}", c.element());
				return;
			}
		}
	}

	@Override
	public PCollection<Set<Integer>> apply(PInput input) {
		return input.getPipeline()
				.apply(TextIO.Read.named("Read_" + name).from(path))
				.apply("ConvertToInts_" + name, ParDo.of(new ExtractSeedDoFn()))
				.apply("Unify_" + name, Combine.globally(new SetUnionFn()));
	}

	public static class SetUnionFn extends CombineFn<Integer, Set<Integer>, Set<Integer>> {
		@Override
		public Set<Integer> createAccumulator() { return new HashSet<>(); }
		@Override
		public Set<Integer> addInput(Set<Integer> accum, Integer input) {
			accum.add(input);
			return accum;
		}
		@Override
		public Set<Integer> mergeAccumulators(Iterable<Set<Integer>> accums) {
			Set<Integer> merged = createAccumulator();
			for (Set<Integer> accum : accums) {
				merged.addAll(accum);
			}
			return merged;
		}
		@Override
		public Set<Integer> extractOutput(Set<Integer> accum) {
			return accum;
		}
	}
}
