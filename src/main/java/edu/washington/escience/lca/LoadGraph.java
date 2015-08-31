package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

public class LoadGraph extends PTransform<PInput, PCollection<KV<Integer, Integer>>> {

	/***/
	private static final long serialVersionUID = 1L;
	private final String name;
	private final String path;
	private final boolean destFirst;

	public LoadGraph(String name, String path, boolean destFirst) {
		this.name = name;
		this.path = path;
		this.destFirst = destFirst;
	}

	public static class ExtractLinkDoFn extends DoFn<String, KV<Integer, Integer>> {
		/***/
		private static final long serialVersionUID = 1L;
		private final boolean destFirst;

		public ExtractLinkDoFn(boolean destFirst) {
			this.destFirst = destFirst;
		}


		@Override
		public void processElement(ProcessContext c) throws Exception {
			String[] fields = c.element().split("\\s", 2);
			if (fields.length != 2 || fields[0] == null || fields[1] == null) {
				return;
			}
			try {
				if (destFirst) {
					c.output(KV.of(Integer.parseInt(fields[1]), Integer.parseInt(fields[0])));
				} else {
					c.output(KV.of(Integer.parseInt(fields[0]), Integer.parseInt(fields[1])));
				}
			} catch (NumberFormatException e) {
				return;
			}
		}
	}

	@Override
	public PCollection<KV<Integer, Integer>> apply(PInput input) {
		return input.getPipeline()
				.apply(TextIO.Read.named("Read" + name).from(path))
				.apply("ConvertToInts", ParDo.of(new ExtractLinkDoFn(destFirst)));
	}

}
