package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class PaperPair {
	int p1;
	int p2;

	public static PaperPair of(int p1, int p2) {
		return new PaperPair(p1, p2);
	}

	/////////////////////////////////////////////////////////////////////////////
	public PaperPair() {}
	private PaperPair(int p1, int p2) {
		this.p1 = p1;
		this.p2 = p2;
	}
}