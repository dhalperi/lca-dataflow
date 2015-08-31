package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.values.TupleTag;

public class TupleTagUtil {
	static <T> TupleTag<T> makeTag() {
		return new TupleTag<T>() {
			private static final long serialVersionUID = 1L;
		};
	}
}
