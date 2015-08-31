package edu.washington.escience.lca;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Vertex {
	public final int id;
	public final int year;

	private Vertex(int id, int year) {
		this.id = id;
		this.year = year;
	}

	public static Vertex of(int id, int year) {
		return new Vertex(id, year);
	}

	private Vertex() {
		this(-1, -1);
	}
}
