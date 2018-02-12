package org.apache.giraph.subgraph;

import java.util.Collection;

public interface BasicGraphStoreInterface {

	
	/* edge access interface */
	public BasicEdge getEdge(int first, int second);
	public BasicEdge getMainEdge(int first, int second);
	public Collection<BasicEdge> getEdges();
	
	/**
	 * If the edge is exist, just leave the edge unchanged.
	 * true: add successfully
	 * false: fail to add
	 * @param first
	 * @param second
	 * @return
	 */
	public boolean addEdge(BasicEdge re);
	
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
	
	/* vertex access interface */
	public BasicVertex getVertex(int id);
	public int getDegree(int id);
	public boolean isLocal(int id);
	public Collection<BasicVertex> getLocalVertex();
//	public boolean isSamePartition(int vid1, int vid2);
	public long getVertexCount();
	public long getEdgeCount();
	
	public long getTotalEdgeCount();
	
	/* initialize */
	public void inititalGraphStore();
	
	/* debug */
	public void dump();
}
