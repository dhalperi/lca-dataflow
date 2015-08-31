package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Link {
	final int src;
	final int dst;

	private Link(int src, int dst) {
		this.src = src;
		this.dst = dst;
	}

	public static Link of(int src, int dst) {
		return new Link(src, dst);
	}

	private Link() {
		this(-1, -1);
	}
}
