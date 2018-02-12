package org.apache.giraph.subgraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.SimplePartition;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class SimpleGraphStore 
extends SimplePartition<IntWritable, IntWritable, NullWritable, TripleWritable>
implements BasicGraphStoreInterface
{

	private static final Logger LOG = Logger.getLogger(SimpleGraphStore.class);
	
//	private HashMap<Long, RawEdge> edgeIndex; /* record the edge existence */
//	private HashMap<Integer, RawVertex> localVertexIndex; 
//	private HashMap<Integer, RawVertex> remoteVertexIndex;
	
	/**
	 * Index the local vertex, which stores the edge <L,L>,<L,R>
	 * The edge property is only stored in the local graph. 
	 * For <L, L> the property is stored in ordered.
	 * For <L, R> is only one copy here.
	 */
	private HashMap<Integer, BasicVertex> localVertexIndex; 
	/**
	 * Index the remote vertex, which stores the edge <L,R>, <R,R>
	 * store it for the triangle completeness.
	 * These edges are only used for the connection, 
	 * not for the property access
	 */
	private HashMap<Integer, BasicVertex> remoteVertexIndex; 
	
	public SimpleGraphStore() { }
	
	public void initialize(int partitionId, Progressable progressable) {
		super.initialize(partitionId, progressable);
		
//		edgeIndex =  new HashMap<Long, RawEdge>();
//		localVertexIndex = new HashMap<Integer, RawVertex>();
//		remoteVertexIndex = new HashMap<Integer, RawVertex>();
		localVertexIndex = new HashMap<Integer, BasicVertex>();
		remoteVertexIndex = new HashMap<Integer, BasicVertex>();
	}

	/**
	 * Return the count of local vertex
	 */
	public long getVertexCount(){
		return localVertexIndex.size();
	}
	
	/**
	 * Return the count of cross edge and internal edge
	 */
	public long getEdgeCount(){
		int size = 0;
		for(BasicVertex rv : localVertexIndex.values()){
			size += rv.getNeighbors().size();
		}
		return size;
	}
	
	public long getTotalEdgeCount(){
		int size = 0;
		size += getEdgeCount();
		for(BasicVertex rv : remoteVertexIndex.values()){
			size += rv.getNeighbors().size();
		}
		return size;
	}
	
	public boolean edgeExist(int first, int second){
		BasicVertex bv = getVertex(first);
		if(bv == null)
			return false;
		return bv.containNeighbor(second);
	}
	
	/**
	 * Must return the Main Copy with supported value.
	 * @param first
	 * @param second
	 * @return
	 */
	public BasicEdge getMainEdge(int first, int second) {
		int endA = (first < second ? first : second);
		int endB = (first == endA ? second : first);
		if( localVertexIndex.containsKey(endA) ){
			return localVertexIndex.get(endA).getNeighbor(endB);
		}
		return localVertexIndex.get(endB).getNeighbor(endA);
	}
	
	/**
	 * if main edge exist, then return the main edge;
	 * else return the random edge;
	 */
	public BasicEdge getEdge(int first, int second){
		if( localVertexIndex.containsKey(first) || localVertexIndex.containsKey(second)){
			return getMainEdge(first, second);
		}
		return remoteVertexIndex.get(second).getNeighbor(first);
	}

	/**
	 * NOTE: Inefficient currently. 
	 * @return
	 */
	public Collection<BasicEdge> getEdges() {
		Collection<BasicEdge> res = new ArrayList<BasicEdge>();
		for(BasicVertex vertex : localVertexIndex.values()){
			for(BasicEdge edge : vertex.getNeighbors()){
				if(edge.isMainCopy()){
					res.add(edge);
				}
			}
		}
		return res;
	}

	/**
	 * We only called for add the remote edges(EXTERNAL)
	 * @param re
	 * @return
	 */
	public boolean addEdge(BasicEdge re) {
		/* 1. no duplication
		 * 2. check the end point is exist or not */
		int enda = re.getSourceId();
		int endb = re.getTargetId();
		
		if(this.edgeExist(enda, endb)){
//			LOG.info("Add external edge ("+ re.getSourceId()+", "+ re.getTargetId()+") is existed");
			return false;
		}

		/* update the neighborhood of vertex */
		BasicVertex tmp = remoteVertexIndex.get(enda);
		if( tmp == null){
			tmp = new BasicVertex();
			tmp.initialize(enda);
			remoteVertexIndex.put(enda, tmp);
		}
		/* internal edge would be calculate two times */
		if(!tmp.containNeighbor(endb))
			tmp.addNeighbor(new BasicEdge(enda, endb, BasicEdge.BASICEDGE_EXTERNAL, true));
		
		tmp = remoteVertexIndex.get(endb);
		if( tmp == null){
			tmp = new BasicVertex();
			tmp.initialize(endb);
			remoteVertexIndex.put(endb, tmp);
		}
		/* internal edge would be calculate two times */
		if(!tmp.containNeighbor(enda))
			tmp.addNeighbor(new BasicEdge(endb, enda, BasicEdge.BASICEDGE_EXTERNAL, true));
		
		return true;
	}

	/**
	 * Need to delete two copy
	 * @param first
	 * @param second
	 * @return
	 */
	public boolean deleteEdge(int first, int second) {
		
		/* first copy */
		if(localVertexIndex.containsKey(first)){
			this.removeInLocalVertexIndex(first, second);
		}
		else{
			this.removeInRemoteVetexIndex(first, second);
		}
		
		/* second copy */
		if(localVertexIndex.containsKey(second)){
			this.removeInLocalVertexIndex(second, first);
		}
		else{
			this.removeInRemoteVetexIndex(second, first);
		}
		return true;
	}
	
	private void removeInLocalVertexIndex(int sourceId, int targetId){
		BasicVertex rv = localVertexIndex.get(sourceId);
		rv.removeEdge(targetId);
		if(rv.getDegree() == 0){
			localVertexIndex.remove(sourceId);
		}
	}
	
	private void removeInRemoteVetexIndex(int sourceId, int targetId){
		BasicVertex rv = remoteVertexIndex.get(sourceId);
		rv.removeEdge(targetId);
		if(rv.getDegree() == 0){
			remoteVertexIndex.remove(sourceId);
		}
	}

	public BasicVertex getVertex(int id) {
		BasicVertex rv = localVertexIndex.get(id);
		if(rv == null){
			rv = remoteVertexIndex.get(id);
		}
		return rv;
	}


//	public boolean isSamePartition(int vid1, int vid2) {
//		int pn = getConf().getInt(GiraphConstants.USER_PARTITION_COUNT.getKey(), 1);
//		
//		return ( vid1 % pn ) == ( vid2 % pn );
//	}

	public void dump(){
		LOG.info("");
		LOG.info("Vertex Size="+ this.getVertexCount());
		for(BasicVertex rv : this.getLocalVertex()){
			LOG.info("\tLocal: "+rv.toString());
		}
		for(BasicVertex rv : this.remoteVertexIndex.values()){
			LOG.info("\tRemote: "+rv.toString());
		}
		LOG.info("Edge Size="+this.getEdgeCount());
		for(BasicEdge re : this.getEdges()){
			LOG.info("\t" + re.toString());
		}
		LOG.info("");
	}

	/**
	 * Called after input superstep finished
	 */
	public void inititalGraphStore() {
		for(Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> vertex : this.getVertexs()){
			/* create vertex */
//			LOG.info("vertex info:"+vertex.toString());
			int vid = vertex.getId().get();
			BasicVertex rv = remoteVertexIndex.get(vid);
			if(rv == null){
				rv = new BasicVertex();
				rv.initialize(vid);
			}
			else{
				remoteVertexIndex.remove(vid);
			}
			localVertexIndex.put(vid, rv);
			
			/* add neighbor, and add edge */
			BasicVertex tmp;
			for(Edge<IntWritable, NullWritable> edge : vertex.getEdges()){
				int targetId = edge.getTargetVertexId().get();
//				LOG.info(" target Id="+ targetId);
				tmp = localVertexIndex.get(targetId);
				if(tmp != null){
					/* set main copy according to the order */
					tmp.getNeighbor(vid).setMainCopy(targetId < vid);
					tmp.getNeighbor(vid).setEdgeType(BasicEdge.BASICEDGE_INTERNAL);
					rv.getNeighbor(targetId).setMainCopy(vid < targetId);
					rv.getNeighbor(targetId).setEdgeType(BasicEdge.BASICEDGE_INTERNAL);
					continue;
				}
				tmp = remoteVertexIndex.get(targetId);
				if(tmp == null){
					tmp = new BasicVertex();
					tmp.initialize(targetId);
					remoteVertexIndex.put(targetId, tmp);
				}
				rv.addNeighbor(new BasicEdge(vid, targetId, BasicEdge.BASICEDGE_CROSS, true));
				tmp.addNeighbor(new BasicEdge(targetId, vid, BasicEdge.BASICEDGE_CROSS, false));
			}
		}
		
//		LOG.info("After initialization the graph store.");
//		dump();
	}

	public int getDegree(int id) {
		if(getVertex(id) == null){
			return 0;
		}
		return getVertex(id).getDegree();
	}

	public boolean isLocal(int id) {
		return (localVertexIndex.get(id) != null);
	}

	public Collection<BasicVertex> getLocalVertex() {
		return this.localVertexIndex.values();
	}
	

//	@Override
//	public Iterator<Vertex<IntWritable, IntWritable, NullWritable, TripleWritable>> iterator() {
//		ConcurrentMap vertexMap2 = Maps.newConcurrentMap();
//		for(BasicVertex rv : localVertexIndex.values()){
//			Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> vertex = getConf().createVertex();
//			List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
//			for(BasicEdge nb : rv.getNeighbors()){
//				edges.add(EdgeFactory.create(new IntWritable(nb.getTargetId()), NullWritable.get()));
//			}
//			vertex.initialize(new IntWritable(rv.getId()), new IntWritable(0), edges);
//			vertexMap2.put(vertex.getId(), vertex);
//		}
//		return vertexMap2.values().iterator();
//	}

}
