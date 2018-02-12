package org.apache.giraph.tools.utils;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * specially for the NeighbourList intersection
 * adapt from BitSet implementation.
 * @author simon0227
 *
 */
public class BitMap {
    /*
     * BitSets are packed into arrays of "words."  Currently a word is
     * a long, which consists of 64 bits, requiring 6 address bits.
     * The choice of word size is determined purely by performance concerns.
     */
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;
	
	
	private long[] words;
//	private int maxWordsSize;
	private transient int wordInUse;
	
//	public int listCount = 0;
//	public int bitmapCount = 0;
	
	/*
	 * Construct with initial size;
	 */
	public BitMap(int size){
//		initialWords(size);
		words = new long[size];
		wordInUse = 0;
	}
	
	public BitMap() {
		this(1);
//		wordInUse = 0;
	}
	
	protected void initialWords(int size){
//		words = null;
		words = new long[size];
//		maxWordsSize = size;
	}
	
	
	public void set(int bitIndex){
		if (bitIndex < 0)
		 throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
		
		int wordIndex = wordIndex(bitIndex);
		expandTo(wordIndex);
		
		words[wordIndex] |= (1L << bitIndex); // Restores invariants
		
	}
	
    public boolean get(int bitIndex) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

//        checkInvariants();

        int wordIndex = wordIndex(bitIndex);
        return (wordIndex < wordInUse)
            && ((words[wordIndex] & (1L << bitIndex)) != 0);
    }    
    
    public int wordsLength() {
    	return wordInUse;
    }
    
//    public int length() {
//        if (wordInUse == 0)
//            return 0;
//
//        return BITS_PER_WORD * (wordsInUse - 1) +
//            (BITS_PER_WORD - Long.numberOfLeadingZeros(words[wordsInUse - 1]));
//    }
    
	public int cardinality(){
        int sum = 0;
        for (int i = 0; i < wordInUse; i++)
            sum += Long.bitCount(words[i]);
        return sum;
	}
	
	public int andCount(BitMap bm, ArrayList<Integer> elelist){
        if (this == bm)
            throw new IndexOutOfBoundsException("same BitMap");

        int sum = 0;
        int indexLimit = wordInUse > bm.wordInUse ? bm.wordInUse : wordInUse;
        // Perform logical AND on words in common
        if(indexLimit <= elelist.size()) {
//        	bitmapCount++;
        	for (int i = 0; i < indexLimit; i++)
        		sum += Long.bitCount(words[i] & bm.words[i]);
        }
        else{
//        	listCount++;
        	for(Integer ele : elelist){
        		if(get(ele) == true) sum++;
        	}
        }
        return sum;
	}
	
	public ArrayList<Integer> getIndexList(){
		if(wordInUse == 0)
			return null;
		ArrayList<Integer> al = new ArrayList<Integer>();
		for(int wordIndex = 0; wordIndex < wordInUse; ++wordIndex){
			Long word = words[wordIndex];
			int cnt = 0;
			while(word > 0){
				if((word & 1L) == 1){
					al.add((wordIndex << ADDRESS_BITS_PER_WORD) + cnt);
				}
				word >>= 1L;
				cnt++;
			}
		}
		return al;
	}
	
    /**
     * Given a bit index, return word index containing it.
     */
    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }
    
    /**
     * Ensures that the BitSet can hold enough words.
     * @param wordsRequired the minimum acceptable number of words.
     */
    private void ensureCapacity(int wordsRequired) {
        if (words.length < wordsRequired) {
            // Allocate larger of doubled size or required size
            int request = Math.max(2 * words.length, wordsRequired);
            words = Arrays.copyOf(words, request);
//            sizeIsSticky = false;
        }
    }

    /**
     * Ensures that the BitSet can accommodate a given wordIndex,
     * temporarily violating the invariants.  The caller must
     * restore the invariants before returning to the user,
     * possibly using recalculateWordsInUse().
     * @param wordIndex the index to be accommodated.
     */
    private void expandTo(int wordIndex) {
        int wordsRequired = wordIndex+1;
        if (wordInUse < wordsRequired) {
            ensureCapacity(wordsRequired);
            wordInUse = wordsRequired;
        }
    }

	public void clear() {
        while (wordInUse > 0)
            words[--wordInUse] = 0;
	}
	
	public long getWord(int idx){
		return words[idx];
	}
	
	public void setWord(int idx, long value){
		words[idx] = value;
	}
	
	public void setWordLength(int size){
		wordInUse = size;
	}
	
	public String toString(){
		ArrayList<Integer> al = getIndexList();
		String ans = "";
		for(int value : al){
			ans += value +", ";
		}
		return ans;
	}
}
