package org.apache.giraph.partition;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.PartitionExchange;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.Random;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import org.apache.log4j.Logger;

public class IterativeStreamWorkerPartitioner <I extends WritableComparable, 
V extends Writable, E extends Writable, M extends Writable>
implements  WorkerGraphPartitioner<I, V, E, M> {

	/** class logger */
	private static final Logger LOG = Logger.getLogger(IterativeStreamWorkerPartitioner.class);
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	
	protected List<PartitionOwner> partitionOwnerList =
	       Lists.newArrayList();
	
	private short[] partitionMap = null;
	
	private LDGHeuristicRule rule = null;

	public IterativeStreamWorkerPartitioner(ImmutableClassesGiraphConfiguration conf) {
		rule = new LDGHeuristicRule<I,V,E,M>(conf);
		partitionMap = new short[rule.graphVertexNumber];
		for(int i = 0; i < rule.graphVertexNumber; i++){
			partitionMap[i] = -1;
		}
	}
	
	@Override
	/** in order to generate stub.*/
	public PartitionOwner createPartitionOwner() {
		return new BasicPartitionOwner();
	}

	/**
	 * This method does not work correctly, because the partitionMap 
	 * has not been synchronized after input superstep.
	 */
	@Override
	public PartitionOwner getPartitionOwner(I vertexId) {
		int index = partitionMap[Integer.valueOf(vertexId.toString())];
		if(index == -1) index = 0;
		return partitionOwnerList.get(index);
	}
	
	
	@Override
	public Collection<PartitionStats> finalizePartitionStats(
			Collection<PartitionStats> workerPartitionStats,
			PartitionStore<I, V, E, M> partitionStore) {
	    // No modification necessary
	    return workerPartitionStats;
	}

	@Override
	public PartitionExchange updatePartitionOwners(WorkerInfo myWorkerInfo,
			Collection<? extends PartitionOwner> masterSetPartitionOwners,
			PartitionStore<I, V, E, M> partitionStore) {
	    partitionOwnerList.clear();
	    partitionOwnerList.addAll(masterSetPartitionOwners);	   

	    Set<WorkerInfo> dependentWorkerSet = new HashSet<WorkerInfo>();
	    Map<WorkerInfo, List<Integer>> workerPartitionOwnerMap =
	        new HashMap<WorkerInfo, List<Integer>>();
	    for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
	      if (partitionOwner.getPreviousWorkerInfo() == null) {
	        continue;
	      } else if (partitionOwner.getWorkerInfo().equals(
	          myWorkerInfo) &&
	          partitionOwner.getPreviousWorkerInfo().equals(
	              myWorkerInfo)) {
	        throw new IllegalStateException(
	            "updatePartitionOwners: Impossible to have the same " +
	                "previous and current worker info " + partitionOwner +
	                " as me " + myWorkerInfo);
	      } else if (partitionOwner.getWorkerInfo().equals(myWorkerInfo)) {
	        dependentWorkerSet.add(partitionOwner.getPreviousWorkerInfo());
	      } else if (partitionOwner.getPreviousWorkerInfo().equals(
	          myWorkerInfo)) {
	        if (workerPartitionOwnerMap.containsKey(
	            partitionOwner.getWorkerInfo())) {
	          workerPartitionOwnerMap.get(
	              partitionOwner.getWorkerInfo()).add(
	                  partitionOwner.getPartitionId());
	        } else {
	          List<Integer> tmpPartitionOwnerList = new ArrayList<Integer>();
	          tmpPartitionOwnerList.add(partitionOwner.getPartitionId());
	          workerPartitionOwnerMap.put(partitionOwner.getWorkerInfo(),
	                                      tmpPartitionOwnerList);
	        }
	      }
	    }
	
	    return new PartitionExchange(dependentWorkerSet,
	        workerPartitionOwnerMap);
	}

	@Override
	public Collection<? extends PartitionOwner> getPartitionOwners() {
	    return partitionOwnerList;
	}

	/**
	 * determine the partitionId(= partitionOwner index) for the vertex,
	 * based on neighbor information by using some heuristic rules. 
	 * 
	 * Note: The vertex must have not been processed.
	 */
	public PartitionOwner determinePartitionId(Vertex<I, V, E, M> vertex) {
		short pid = rule.makeDecision(vertex);
		partitionMap[Integer.valueOf(vertex.getId().toString())] = pid; 
		return partitionOwnerList.get(pid);
	}
	
	public void savePartitionMap(){
		for(int i = 0; i < rule.graphVertexNumber; i++){
			if(partitionMap[i] != -1)
				System.err.println(i + " " + partitionMap[i]);
		}
	}
	
	/**
	 * 
	 * @author ggbond
	 *
	 */
	class LDGHeuristicRule<I extends WritableComparable, 
	V extends Writable, E extends Writable, M extends Writable>{

		private final Logger LOG = Logger.getLogger(LDGHeuristicRule.class);
		
		private short [] prePartitionMap = null;
		private String partitionMapPath;
		private int graphVertexNumber;
		private long graphTotalWorkload;
		
		
		private int [] neighbor;
		private int [] candidate;
		private int partitionNumber;
		private HashMap<Integer, Long> workloadTracker = new HashMap<Integer, Long>();
		private long workloadCapacityPerWorker;

		public LDGHeuristicRule(ImmutableClassesGiraphConfiguration conf) {
			this.partitionMapPath = conf.get(GiraphConstants.PARTITION_MAP_PATH, GiraphConstants.DEFAULT_PARTITION_MAP_PATH);
			this.graphVertexNumber = conf.getInt(GiraphConstants.GRAPH_VERTEX_NUMBER, GiraphConstants.DEFAULT_GRAPH_VERTEX_NUMBER);
			this.graphTotalWorkload = Long.valueOf(conf.get(GiraphConstants.GRPAH_TOTAL_WORKLOAD, GiraphConstants.DEFAULT_GRAPH_TOTAL_WORKLOAD));
			if("" != partitionMapPath)
				this.initialization(conf);
			
			partitionNumber = conf.getInt(GiraphConstants.MAX_WORKERS, 60);
			workloadCapacityPerWorker = (graphTotalWorkload + partitionNumber)/partitionNumber;
			neighbor = new int[partitionNumber];
			candidate = new int[partitionNumber];
			for(int i = 0; i < partitionNumber; i++){
				workloadTracker.put(i, 0L);
			}
			LOG.info("partitionNumber="+partitionNumber+" workloadCapacityPerWorker="+workloadCapacityPerWorker);
			
		}
		
		public short makeDecision(Vertex<I, V, E, M> vertex){
			/*
			 * 1. iterate neighbors and get each one's partition id.
			 * 2. determine the partition id of vertex based on heuristic rule.
			 * 3. update partitionMetadataClient 
			 */
			double score = Double.NEGATIVE_INFINITY;
			int candidateSize = partitionNumber;
			long workload = 0;
			double tmpScore;
			
			for(int i = 0; i < partitionNumber; i++){
				neighbor[i] = 0;
			}
			for(Edge<I,E> edge : vertex.getEdges()){
				if(prePartitionMap != null)
					neighbor[prePartitionMap[edge.getTargetVertexId().hashCode()]]++;
				else{
					neighbor[edge.getTargetVertexId().hashCode() % partitionNumber]++;
				}
			}
			
			for(int i = 0; i < partitionNumber; i++){
				workload = workloadTracker.get(i);
				tmpScore = neighbor[i] * (1.0 - workload / workloadCapacityPerWorker);
				if(Double.compare(tmpScore, score) > 0){
					candidate[0] = i;
					candidateSize = 1;
					score = tmpScore;
				}
				else if(Math.abs(tmpScore-score) < 1e-5){
					candidate[candidateSize++] = i;
				}
				if(LOG.isDebugEnabled()){
					LOG.debug("VertexId="+vertex.getId().hashCode() +":  pid="+i+" workload="
				+workload+" neighborsize="+neighbor[i]+" tmpscore="+tmpScore+" maxScore="+score+
							" candidateSize="+candidateSize);
				}
			}
			int selectedPid = candidate[(Math.abs(Random.nextInt()) % candidateSize)];
			workload = workloadTracker.get(selectedPid) + vertex.getNumEdges();
			workloadTracker.put(selectedPid, workload);
			if(LOG.isDebugEnabled()){
				LOG.debug("VertexId="+vertex.getId().hashCode() +" -> pid="+selectedPid+" new workload="+workload);
			}
			return (short)selectedPid;
		}
		
		/**
		 *  initialize prePartitionMap from already existed partition map. 
		 * @param job
		 */
		public void initialization(Configuration job) {
			Path path = new Path(partitionMapPath); //used to open file by filesystem
			prePartitionMap = new short[graphVertexNumber];
			FileSystem fs = null;
			FSDataInputStream fileIn = null;
		    BufferedReader reader = null;
		    try {
			  fs = path.getFileSystem(job);
		      fileIn = fs.open(path);
		      reader = new BufferedReader(new InputStreamReader(fileIn, Charsets.UTF_8));
		      String line;
		      while ( (line=reader.readLine()) != null) {
				  String[] tokens = SEPARATOR.split(line);
				  prePartitionMap[Integer.valueOf(tokens[0])]=Short.valueOf(tokens[1]);
		      }
		    }catch(IOException e) { }
			finally {
		      Closeables.closeQuietly(fileIn);
		      Closeables.closeQuietly(reader);  
		    }
		}
	}
}