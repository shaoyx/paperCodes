package org.apache.giraph.subgraph;

import java.util.ArrayList;
import java.util.Collection;

public interface GraphStoreInterface {
	
	/* edge access interface */
	public RawEdge getEdge(int first, int second);
	public RawEdge getEdge(long id);
	public Collection<RawEdge> getEdges();
	
	/**
	 * If the edge is exist, just leave the edge unchanged.
	 * true: add successfully
	 * false: fail to add
	 * @param first
	 * @param second
	 * @return
	 */
	public boolean addEdge(RawEdge re);
	
	/**
	 * After delete the edge, if the end point has no neighbor.
	 * Then delete the vertex too.
	 * true: delete successfully
	 * false: fail to delete
	 * @param first
	 * @param second
	 * @return
	 */
	public boolean deleteEdge(int first, int second);
	public boolean edgeExist(int first, int second);
	public boolean edgeExist(long id);
	
	/* vertex access interface */
	public RawVertex getVertex(int id);
	public int getDegree(int id);
	public boolean isLocal(int id);
	public Collection<RawVertex> getLocalVertex();
	public boolean isSamePartition(int vid1, int vid2);
	public long getVertexCount();
	public long getEdgeCount();
	

	public ArrayList<RawVertex> getVertexs();
	public void trim();
	
	/* initialize */
	public void inititalGraphStore();
	
	/* debug */
	public void dump();
}
