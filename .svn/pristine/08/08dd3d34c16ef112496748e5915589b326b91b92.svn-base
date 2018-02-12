package org.apache.giraph.subgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.BasicPartition;
import org.apache.giraph.partition.Partition;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Fix the type here!
 * support random access Edge, Vertex
 * @author simon0227
 *
 */
public class BasicGraphStore 
extends BasicPartition<IntWritable, IntWritable, NullWritable, TripleWritable>
implements GraphStoreInterface{
	
//	private static Logger LOG = Logger.getLogger(BasicGraphStore.class);

//	private ImmutableClassesGiraphConfiguration<IntWritable, IntWritable, NullWritable, TripleWritable> conf;
	
//	private HashMap<Integer, Integer> gvidToLvidMap;
	private ConcurrentMap<Integer, RawVertex> graphStructure; /* index structure */
	private ConcurrentMap<Long, RawEdge> edgeList; /* index structure */
	

	private ConcurrentMap<IntWritable, Vertex<IntWritable, IntWritable, NullWritable, TripleWritable>> vertexMap;
	
	public BasicGraphStore() { }
	
	public void initialize(int partitionId, Progressable progressable) {
		super.initialize(partitionId, progressable);
//		LOG.info("Create a graph store for partition id="+partitionId);
		graphStructure = Maps.newConcurrentMap(); //new HashMap<Integer, RawVertex>();
		edgeList = Maps.newConcurrentMap(); //new HashMap<Long, RawEdge>();
		vertexMap = Maps.newConcurrentMap();
	}
	
	/* Interface for the basic partition */
	@Override
	public Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> getVertex(
			IntWritable vertexIndex) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> putVertex(
			Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> vertex) {
		
//		vertexMap.put(vertex.getId(), vertex);
//		LOG.info("Insert vertex = "+vertex.getId().get()+" into partition id="+this.getId());
		RawVertex rv = graphStructure.putIfAbsent(vertex.getId().get(), new RawVertex());
//		if(graphStructure.get(vertex.getId().get()) == null){
//			 rv = new RawVertex();
//			 graphStructure.put(vertex.getId().get(), rv);
//		}
		if(rv == null)
			rv = graphStructure.get(vertex.getId().get());
		rv.initialize(vertex.getId().get(), true);
		
		RawEdge redge;
		RawVertex tmp;
		for(Edge<IntWritable, NullWritable> edge : vertex.getEdges()){
			int targetId = edge.getTargetVertexId().get();
			tmp = graphStructure.putIfAbsent(targetId, new RawVertex());
//			if( tmp == null){
//				tmp = new RawVertex();
//				tmp.initialize(targetId, false);
//				graphStructure.put(targetId, tmp);
//			}
			if(tmp == null){
				tmp = graphStructure.get(targetId);
				if(!tmp.isLocal())
					tmp.initialize(targetId, false);
			}
			/* internal edge would be calculate two times */
			if(!tmp.containNeighbor(vertex.getId().get()))
				tmp.addNeighbor(vertex.getId().get());
			if(!rv.containNeighbor(targetId))
				rv.addNeighbor(targetId);
			
			redge = edgeList.putIfAbsent(RawEdge.constructEdgeId(vertex.getId().get(), targetId), new RawEdge());//edgeList.get(RawEdge.constructEdgeId(vertex.getId().get(), targetId));
//			if(redge == null){
//				redge = new RawEdge();
//				edgeList.put(RawEdge.constructEdgeId(vertex.getId().get(), targetId), redge);
//				redge.initialize(vertex.getId().get(), targetId);
//			}
			if(redge == null){
				redge = edgeList.get(RawEdge.constructEdgeId(vertex.getId().get(), targetId));
				redge.initialize(vertex.getId().get(), targetId);
			}
			if(!graphStructure.get(targetId).isLocal()){
				redge.setEdgeType(RawEdge.RAWEDGE_CROSS);
			}
			else{
				redge.setEdgeType(RawEdge.RAWEDGE_INTERNAL);
			}
		}
//		if(LOG.isInfoEnabled()){
//			LOG.info("vertex size="+graphStructure.size()+" edge size="+edgeList.size());
//		}
		return null;
	}
	
	@Override
	public Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> removeVertex(
			IntWritable vertexIndex) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void addPartition(
			Partition<IntWritable, IntWritable, NullWritable, TripleWritable> partition) {
//		LOG.info("Call Add Partition.........");
//		LOG.info("Add Partition: My Partition ID="+this.getId()+" coming partition id="+partition.getId());
		for(Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> v : partition){
//			LOG.info(v.toString());
			this.putVertex(v);
		}
	}
	@Override
	public long getVertexCount() {
		int size = 0;
		for(RawVertex rv : graphStructure.values()){
			if(rv.isLocal())
				size++;
		}
		return size;
	}
	@Override
	public long getEdgeCount() {
	    long edges = 0;
		for(RawVertex rv : graphStructure.values()){
			if(rv.isLocal())
				edges += rv.getNeighbors().size();
		}
	    return edges;
	}
	@Override
	public void saveVertex(
			Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> vertex) {
		
	}
	 @Override
	  public void readFields(DataInput input) throws IOException {
	    super.readFields(input);
	    int size = input.readInt();
	    graphStructure = Maps.newConcurrentMap(); //new HashMap<Integer, RawVertex>();
	    edgeList = Maps.newConcurrentMap(); //new HashMap<Long, RawEdge>();
	    for(int i = 0; i < size; i++){
	    	RawVertex rv = new RawVertex();
	    	rv.readFields(input);
	    	graphStructure.put(rv.getId(), rv);
	    }
	    
	    size = input.readInt();
	    for(int i = 0; i < size; i++){
	    	RawEdge re = new RawEdge();
	    	re.readFields(input);
	    	edgeList.put(RawEdge.constructEdgeId(re.getSmallEndPoint(), re.getBigEndPoint()), re);
	    }
	  }
	
	  @Override
	  public void write(DataOutput output) throws IOException {
	    super.write(output);
	    output.writeInt(graphStructure.size());
	    for(RawVertex rv : graphStructure.values()) {
	    	rv.write(output);
	    }
	    output.writeInt(edgeList.size());
	    for(RawEdge re : edgeList.values()){
	    	re.write(output);
	    }
	  }
	@Override
	public Iterator<Vertex<IntWritable, IntWritable, NullWritable, TripleWritable>> iterator() {
		vertexMap.clear();
		for(RawVertex rv : graphStructure.values()){
			if(rv.isLocal()){
				Vertex<IntWritable, IntWritable, NullWritable, TripleWritable> vertex = getConf().createVertex();
				List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
				for(int vid : rv.getNeighbors()){
					edges.add(EdgeFactory.create(new IntWritable(vid), NullWritable.get()));
				}
				vertex.initialize(new IntWritable(rv.getId()), new IntWritable(0), edges);
				vertexMap.put(vertex.getId(), vertex);
			}
		}
		return vertexMap.values().iterator();
	}
	
	/** Interface for the graph store **/
	@Override
	public RawEdge getEdge(int first, int second) {
		return edgeList.get(RawEdge.constructEdgeId(first, second));
	}
	
	@Override
	public Collection<RawEdge> getEdges() {
		return edgeList.values();
	}
	@Override
	public boolean addEdge(RawEdge re) {
		int enda = re.getSmallEndPoint();
		int endb = re.getBigEndPoint();

		RawVertex tmp = graphStructure.get(enda);
		if( tmp == null){
			tmp = new RawVertex();
			tmp.initialize(enda, false);
			graphStructure.put(enda, tmp);
		}
		/* internal edge would be calculate two times */
		if(!tmp.containNeighbor(endb))
			tmp.addNeighbor(endb);
		
		tmp = graphStructure.get(endb);
		if( tmp == null){
			tmp = new RawVertex();
			tmp.initialize(endb, false);
			graphStructure.put(endb, tmp);
		}
		/* internal edge would be calculate two times */
		if(!tmp.containNeighbor(enda))
			tmp.addNeighbor(enda);
		
		long id = RawEdge.constructEdgeId(enda, endb);
		if(edgeList.get(id) != null)
			return false;
		edgeList.put(id, re);
		return true;
	}
	@Override
	public boolean deleteEdge(int first, int second) {
		long id = RawEdge.constructEdgeId(first, second);
		if(edgeList.get(id) == null)
			return false;
		edgeList.get(id).delete();
		return true;
	}
	@Override
	public RawVertex getVertex(int id) {
		return graphStructure.get(id);
	}
	@Override
	public ArrayList<RawVertex> getVertexs() {
		return new ArrayList<RawVertex>(graphStructure.values());
	}

	public void dump(){
//		LOG.info("");
//		LOG.info("Vertex Size="+graphStructure.size());
//		for(RawVertex rv : graphStructure.values()){
//			LOG.info("\t"+rv.toString());
//		}
//		LOG.info("Edge Size="+edgeList.size());
//		for(RawEdge re : edgeList.values()){
//			LOG.info("\t" + re.toString());
//		}
//		LOG.info("");
	}

	/**
	 * Currently suite hash partition.
	 */
	@Override
	public boolean isSamePartition(int vid1, int vid2) {
		int pn = getConf().getInt(GiraphConstants.USER_PARTITION_COUNT.getKey(), 1);
		return ( vid1 % pn ) == ( vid2 % pn );
	}

	/**
	 * only trim vertex here.
	 */
	@Override
	public void trim() {
//		LOG.info("before trim");
//		dump();
		ArrayList<RawVertex> removeList = new ArrayList<RawVertex>();
		for(RawVertex rv : graphStructure.values()){
			for(int nvid : (ArrayList<Integer>)rv.getNeighbors().clone()){
				if(edgeList.get(RawEdge.constructEdgeId(rv.getId(), nvid)).isDelete()){
//					LOG.info("Before remove a neighbor size="+rv.getNeighbors().size());
					rv.getNeighbors().remove(Integer.valueOf(nvid));
//					LOG.info("Before remove a neigbhor size="+rv.getNeighbors().size());
				}
			}
			if(rv.getNeighbors().size() == 0){
				removeList.add(rv);
			}
		}
//		LOG.info("remove size="+removeList.size());
		for(RawVertex rv : removeList){
//			LOG.info("remove id="+rv.getId());
			graphStructure.remove(rv.getId());
		}
//		LOG.info("After trim");
//		dump();
	}

	@Override
	public void inititalGraphStore() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean edgeExist(int first, int second) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean edgeExist(long id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getDegree(int id) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isLocal(int id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ArrayList<RawVertex> getLocalVertex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RawEdge getEdge(long id) {
		return this.edgeList.get(id);
	}

	@Override
	public boolean hasVertex(IntWritable vid) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}
}
