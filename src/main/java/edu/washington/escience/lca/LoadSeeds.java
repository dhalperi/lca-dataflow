package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

public class LoadSeeds extends PTransform<PInput, PCollection<Integer>> {

	/***/
	private static final long serialVersionUID = 1L;
	private final String name;
	private final String path;

	public LoadSeeds(String name, String path) {
		this.name = name;
		this.path = path;
	}

	public static class ExtractSeedDoFn extends DoFn<String, Integer> {
		/***/
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) throws Exception {
			try {
				c.output(Integer.parseInt(c.element()));
			} catch (NumberFormatException e) {
				return;
			}
		}
	}

	@Override
	public PCollection<Integer> apply(PInput input) {
		return input.getPipeline()
				.apply(TextIO.Read.named("Read_" + name).from(path))
				.apply("ConvertToInts_" + name, ParDo.of(new ExtractSeedDoFn()));
	}

}
