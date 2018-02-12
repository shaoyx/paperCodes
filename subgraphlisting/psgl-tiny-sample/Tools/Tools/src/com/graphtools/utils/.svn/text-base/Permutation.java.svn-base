package com.graphtools.utils;

import java.util.Arrays;

public class Permutation {
	// simply prints all permutation - to see how it works
	/**
	 * In order to retrieve all permutations, 
	 * the start permutation should be least order.
	 * @param c
	 */
	public static void printPermutations( Comparable[] c ) {
		System.out.println( Arrays.toString( c ) );
		while ( ( c = nextPermutation( c ) ) != null ) {
			System.out.println( Arrays.toString( c ) );
		}
	}

	// modifies c to next permutation or returns null if such permutation does not exist
	public static Comparable[] nextPermutation( final Comparable[] c ) {
		// 1. finds the largest k, that c[k] < c[k+1]
		int first = getFirst( c );
		if ( first == -1 ) return null; // no greater permutation
		// 2. find last index toSwap, that c[k] < c[toSwap]
		int toSwap = c.length - 1;
		while ( c[ first ].compareTo( c[ toSwap ] ) >= 0 )
			--toSwap;
		// 3. swap elements with indexes first and last
		swap( c, first++, toSwap );
		// 4. reverse sequence from k+1 to n (inclusive) 
		toSwap = c.length - 1;
		while ( first < toSwap )
			swap( c, first++, toSwap-- );
		return c;
	}

	// finds the largest k, that c[k] < c[k+1]
	// if no such k exists (there is not greater permutation), return -1
	private static int getFirst( final Comparable[] c ) {
		for ( int i = c.length - 2; i >= 0; --i )
			if ( c[ i ].compareTo( c[ i + 1 ] ) < 0 )
				return i;
		return -1;
	}

	// swaps two elements (with indexes i and j) in array 
	private static void swap( final Comparable[] c, final int i, final int j ) {
		final Comparable tmp = c[ i ];
		c[ i ] = c[ j ];
		c[ j ] = tmp;
	}
	
	public static void main(String[] args){
		Integer [] list = new Integer[3];
		for(int i = 0; i < list.length; i++){
//			list[i] = list.length - i - 1;
			list[i] = i;
		}
		//System.out.println("orginal: "+Arrays.toString(list));
		Permutation.printPermutations(list);
	}
}
