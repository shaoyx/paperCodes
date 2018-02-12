package org.apache.giraph.tools.utils;

public class SkewRandom {
	
	public static final java.util.Random rand = new java.util.Random();
	
	public static int randomPick(double[] distribution, double normal){
		double randomNumber = rand.nextDouble();
//		System.out.println("randomNumber="+randomNumber);
		double sum = 0.0;
		for(int i = 0; i < distribution.length; i++){
			sum += 1.0 / (distribution[i]*normal);
//			System.out.println("\ti="+i+": "+distribution[i]+", normal="+normal+", sum="+sum);
			if(sum > randomNumber){
//				System.out.println();
				return i;
			}
		}
		return distribution.length - 1;
	}
}
