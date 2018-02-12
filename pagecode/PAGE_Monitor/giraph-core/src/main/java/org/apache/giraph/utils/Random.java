package org.apache.giraph.utils;

public class Random {
	
	public final static java.util.Random randomer = new java.util.Random();
	
	public static int nextInt() {
		return randomer.nextInt();
	}

}
