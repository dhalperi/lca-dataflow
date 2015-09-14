package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

@SuppressWarnings("serial")
public class SwapReachable extends DoFn<KV<Integer, Reachable>, KV<Integer, Reachable>> {
	@Override
	public void processElement(ProcessContext c) throws Exception {
		KV<Integer, Reachable> element = c.element();
		Integer p1 = element.getKey();
		Reachable r = element.getValue();
		c.output(KV.of(r.dst, Reachable.of(p1, r.depth)));
	}
}
