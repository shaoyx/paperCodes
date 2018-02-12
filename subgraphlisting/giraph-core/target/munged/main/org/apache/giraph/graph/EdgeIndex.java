package org.apache.giraph.graph;

import java.nio.ByteBuffer;

import org.apache.giraph.utils.bloom.BloomFilter;
import org.apache.giraph.utils.bloom.Key;

public class EdgeIndex 
extends BloomFilter{
	
	public EdgeIndex(){
		super();
	}
	
	public EdgeIndex(int vectorSize, int nbHash, int hashType) {
		super(vectorSize, nbHash, hashType);
	}
	
	public void addEdge(long s, long e){
		byte[] edgeKey = ByteBuffer.allocate(8).putLong((s < e) ? (s << 32L) | e : (e << 32L) | s).array();;
		add(new Key(edgeKey));
	}

}
