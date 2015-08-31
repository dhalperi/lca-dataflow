package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Joiner;

public class StringifyReachable extends DoFn<KV<Integer, Reachable>, String> {
	private static final long serialVersionUID = 1L;

	@Override
	public void processElement(ProcessContext c) throws Exception {
		KV<Integer, Reachable> element = c.element();
		Integer src = element.getKey();
		Reachable r = element.getValue();
		c.output(Joiner.on("\t").join(src, r.dst, r.depth));
	}

}