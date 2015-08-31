package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class GenericLink<V> {
	final V src;
	final V dest;
	
	public GenericLink() {
		src = null;
		dest = null;
	}

	public GenericLink(V src, V dest) {
		this.src = src;
		this.dest = dest;
	}
	
	public V getSrc() {
		return src;
	}

	public V getDest() {
		return dest;
	}

	public static <V> GenericLink<V> of(V src, V dst) {
		return new GenericLink<V>(src, dst);
	}
}
