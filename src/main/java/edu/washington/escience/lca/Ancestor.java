package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Ancestor {
	int id;
	int d1;
	int d2;
	int depth;
	int year;

	public static Ancestor of(int id, int d1, int d2, int year) {
		return new Ancestor(id, d1, d2, year);
	}

	/////////////////////////////////////////////////////////////////////////////
	public Ancestor() {};
	private Ancestor(int id, int d1, int d2, int year) {
		this.id = id;
		this.d1 = d1;
		this.d2 = d2;
		this.depth = Math.max(d1, d2);
		this.year = year;
	}
}
