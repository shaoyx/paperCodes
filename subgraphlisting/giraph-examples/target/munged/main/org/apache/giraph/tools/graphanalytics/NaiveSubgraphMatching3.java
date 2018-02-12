package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexProperty;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.tools.utils.PartialQueryWritable;
import org.apache.giraph.utils.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/*
 * Partial Query Scheme: a node id mapping scheme
 * 
 * <query node id> <data node id>
 * <query node id> <data node id>
 * ...             ...
 * <query node id> <data node id>
 * 
 */

public class NaiveSubgraphMatching3  extends 
Vertex<IntWritable, IntWritable, NullWritable, PartialQueryWritable> {

	private static final Logger LOG = Logger.getLogger(NaiveSubgraphMatching3.class);
	
	private int label;
	private HashMap<Integer, VertexProperty> node2PropertyMap = null;
	
	@Override
	public void compute(Iterable<PartialQueryWritable> messages) throws IOException {
			basic_compute(messages);
	}
	
	/**
	 * basic_compute:
	 * initial policy: manually assigned
	 * distribution policy: random
	 * exploration policy: fully enumeration
	 * @param messages
	 */
	private void basic_compute(Iterable<PartialQueryWritable> messages){
		if(getSuperstep() == 0){
			int initLabel = getInitLabel();
			if(getLabel(getId().get()) == initLabel){
//				LOG.info("QueryGraph:\n"+getQueryGraph().toString());
				int index = getQueryGraph().getNodeIndexByLabel(initLabel);
//				LOG.info("Send initialized info to vertex: "+getId().get() +" label="+getLabel(getId().get()) +" index="+index);
				PartialQueryWritable tmpPQ = new PartialQueryWritable(getQueryGraph().getNodeSize(), getQueryGraph().getTotalEdges());
				tmpPQ.update(index, getId().get());
				tmpPQ.setPreviousMatchedVertexIndex(index);
//				LOG.info("Send PartialQuery:\n"+tmpPQ.toString()+"\nTarget="+getId().get());
				incSendMessageCount();
				sendMessage(getId(), tmpPQ);					
			}
			voteToHalt();
		}
		else{
			HashMap<Integer, ArrayList<Integer>> queryVertexSet = new HashMap<Integer, ArrayList<Integer>>();
			/* 2. enumerate each partialQuery to generate new partialQuery for the next neighbor. */
			for(PartialQueryWritable partialQuery : messages){
				incMessageCount();
//				LOG.info("Process partialQuery:\n"+partialQuery.toString());
				int previousMatchedVertexIndex = partialQuery.getPreviousMatchedVertexIndex();
				ArrayList<Integer> queryGraphEdgeList = getQueryGraph().getEdgeList(previousMatchedVertexIndex);
				boolean valid = true;
				for(Integer neighbor : queryGraphEdgeList){
					int index = getQueryGraph().getNodeIndex(neighbor);
					/* 1, if neighbor has been matched, then we need to verify it is ok. 
					 * called Neighbor connection. */
					if(partialQuery.isMatched(index)){
						valid = checkValid(neighbor, partialQuery.getMappedDataGraphNode(index));
						if(!valid) 
							this.incInvalidByMatchedVertex();
						else if(!partialQuery.isAccessed(index)){
							partialQuery.decRemainEdges();
						}
					}
					/* 2, if neighbor has not been matched, then we generate query vertex set. 
					 */
					else{
						ArrayList<Integer> al = getMatchedList(neighbor, partialQuery);
						if(al == null){
							valid = false;
							this.incInvalidByUnmatchedVertex();
						}
						else{
							queryVertexSet.put(neighbor, al);
							partialQuery.decRemainEdges();
						}
					}
					if(!valid) break;
				}
				if(valid){
					/* generate new partial query. */
					if(queryVertexSet.size() > 0){
						basic_updateAndSendPartialQuery(partialQuery, queryVertexSet);
					}
					else{
//						LOG.info("Valid Result: "+partialQuery.toString());
						if(partialQuery.isCompelete())
							incMatchedSubgraphCount();
						else{
							if(partialQuery.getRemainEdges() < 0){
								System.out.println("Logical error in counting remaineEdges");
								System.exit(-1);
							}
							random_distribute_partial_query(partialQuery);
						}
					}
				}
				queryVertexSet.clear();
			}
			queryVertexSet = null;
			
			/*
			 * After processing current messages, the vertex will vote to halt;
			 */
			voteToHalt();
		}
	}
	

	/**
	 * distribute the partial query to the unmatched node's parent which means trace back.
	 * @param partialQuery
	 */
//	private void random_distribute_partial_query(
//			PartialQueryWritable partialQuery) {
//		ArrayList<Integer> unmatchedIndexList = partialQuery.getUnmatchedList();
//		ArrayList<Integer> candidateTargets = new ArrayList<Integer>();
//		for(int unmatchedIdx : unmatchedIndexList){
//			ArrayList<Integer> neighborList = getQueryGraph().getEdgeList(unmatchedIdx);
//			for(int neighbor : neighborList){
//				if(partialQuery.isMatched(getQueryGraph().getNodeIndex(neighbor))){
//					candidateTargets.add(neighbor);
//					break;
//				}
//			}
//		}
//		
//		int randTarget;
//		do{
//			randTarget = Math.abs(Random.nextInt()) % candidateTargets.size();
//		}while(randTarget < 0);
//		
////		/* 4. send partialQueryWritable */
//		PartialQueryWritable tempPartialQuery = new PartialQueryWritable();
//		tempPartialQuery.copy(partialQuery);
//		tempPartialQuery.setPreviousMatchedVertexIndex(getQueryGraph().getNodeIndex(candidateTargets.get(randTarget)));
////		LOG.info("partialQuery="+partialQuery.toString());
////		LOG.info("Random Send PartialQuery:\n"+tempPartialQuery.toString() +"\nTarget="+partialQuery.getMappedDataGraphNode(getQueryGraph().getNodeIndex(candidateTargets.get(randTarget))));
//		incSendMessageCount();
//		sendMessage(new IntWritable(partialQuery.getMappedDataGraphNode(getQueryGraph().getNodeIndex(candidateTargets.get(randTarget)))), tempPartialQuery);
////		System.exit(1);
//	}
	
	/**
	 * distribute the partial query to the unmatched node's parent which means trace back.
	 * @param partialQuery
	 */
	private void random_distribute_partial_query(
			PartialQueryWritable partialQuery) {
//		ArrayList<Integer> unmatchedIndexList = partialQuery.getUnmatchedList();
//		ArrayList<Integer> unaccessedIndexList = partialQuery.getUnaccessedList();
		ArrayList<Integer> candidateTargets = partialQuery.getUnaccessedList();//new ArrayList<Integer>();
//		for(int unaccessedIdx : unaccessedIndexList){
//			ArrayList<Integer> neighborList = getQueryGraph().getEdgeList(unaccessedIdx);
//			for(int neighbor : neighborList){
//				if(partialQuery.isMatched(getQueryGraph().getNodeIndex(neighbor))){
//					candidateTargets.add(neighbor);
//					break;
//				}
//			}
//		}

		if(candidateTargets.size() == 0) 
			return;
		int randTarget;
		do{
			randTarget = Math.abs(Random.nextInt()) % candidateTargets.size();
		}while(randTarget < 0);
		
//		/* 4. send partialQueryWritable */
		PartialQueryWritable tempPartialQuery = new PartialQueryWritable();
		tempPartialQuery.copy(partialQuery);
		tempPartialQuery.setPreviousMatchedVertexIndex(candidateTargets.get(randTarget));
//		LOG.info("Random Send PartialQuery:\n"+tempPartialQuery.toString() +"\nTarget="+partialQuery.getMappedDataGraphNode(candidateTargets.get(randTarget)));
		incSendMessageCount();
		sendMessage(new IntWritable(partialQuery.getMappedDataGraphNode(candidateTargets.get(randTarget))), tempPartialQuery);
	}
	
	
	private void basic_updateAndSendPartialQuery(PartialQueryWritable pq, HashMap<Integer, ArrayList<Integer>> pr){
		Integer[] queryVertexId = new Integer[pr.keySet().size() + 1];
		Integer[] matchedDataVertex = new Integer[pr.keySet().size() + 1];
		pr.keySet().toArray(queryVertexId);
		recursiveUpdate(pq, queryVertexId, pr, 0, matchedDataVertex);
	}
	
	private void recursiveUpdate(PartialQueryWritable pq, Integer[] keyArray,
			HashMap<Integer, ArrayList<Integer>> pr, int level, Integer[] matchedDataVertex) {
		if(level >= pr.size()){
			PartialQueryWritable tempPartialQuery = new PartialQueryWritable();
			tempPartialQuery.copy(pq);
			int nextIdx;
			do{
				nextIdx = Math.abs(Random.nextInt()) % level;
			}while(nextIdx < 0);
			
			tempPartialQuery.setPreviousMatchedVertexIndex(getQueryGraph().getNodeIndex(keyArray[nextIdx]));
//			LOG.info("Superstep "+getSuperstep()+": "
//			+"Send PartialQuery:\n"+tempPartialQuery.toString()
//			+"\nTargetVertex="+matchedDataVertex[nextIdx]);
//			LOG.info("Send Partial Query: "+tempPartialQuery.toString()+"\nTargetVertex="+matchedDataVertex[nextIdx]);
			incSendMessageCount();
			sendMessage(new IntWritable(matchedDataVertex[nextIdx]), tempPartialQuery);
			return ;
		}
		
		Integer key = keyArray[level];
		int index = getQueryGraph().getNodeIndex(key);
		ArrayList<Integer> candidateSet = pr.get(key);
		
		for(Integer cand : candidateSet){
			pq.update(index, cand);
			matchedDataVertex[level] = cand;
			recursiveUpdate(pq, keyArray, pr, level + 1, matchedDataVertex);
		}
		return ;
	}
	
	public int getLabel(int vid){
		if(this.node2PropertyMap == null){
			/* get it from GraphTaskManager */
			node2PropertyMap = getVertex2PropertyMap();
		}
		return node2PropertyMap.get(vid).getLabel();
	}
	
	public int getDegree(int vid){
		if(this.node2PropertyMap == null){
			node2PropertyMap = getVertex2PropertyMap();
		}
		return node2PropertyMap.get(vid).getDegree();
	}
	
	/**
	 * NOTE: each bit in bm must be exist in vid's bitmap.
	 * @param vid
	 * @param bm
	 * @return
	 */
//	public boolean checkNeighborLabel(int vid, BitMap bm){
//		if(this.node2PropertyMap == null){
//			node2PropertyMap = getVertex2PropertyMap();
//		}
////		LOG.info("Info in VertexProgram:"+bm.toString());
//		return node2PropertyMap.get(vid).checkNeighborLabel(bm);
//	}

	/**
	 * filter by unmatched
	 * use neighbor label index, degree, label, used
	 * @param queryVertexId
	 * @param partialQuery
	 * @return
	 */
	public ArrayList getMatchedList(int queryVertexId, PartialQueryWritable partialQuery){
//		LOG.info("In getMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			/* TODO:
			 * how to conveniently access target vertex Id's label? 
			 * each worker needs a global node id -> label index 
			 */
			if(getLabel(edge.getTargetVertexId().get()) == getQueryGraph().getLabel(queryVertexId) 
					&& !partialQuery.hasUsed(edge.getTargetVertexId().get())
					&& getDegree(edge.getTargetVertexId().get()) >= getQueryGraph().getDegree(queryVertexId)){
				qualifiedNeighbor.add(edge.getTargetVertexId().get());
			}
		}
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
	/**
	 * Check whether edge exist or not?
	 * inefficient implementation.
	 *
	 * @param vid
	 * @return
	 */
	public boolean checkValid(int queryVertexId, int mappedDataVertexId){
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			if(edge.getTargetVertexId().get() == mappedDataVertexId){
				return true;
			}
		}
		return false;
	}
	
	
	/******************************
	 * IO format
	 ******************************/
	/** Vertex InputFormat */
	  public static class NaiveSubgraphMatching3InputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, PartialQueryWritable> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new NaiveSubgraphMatching3Reader();
		}
		
		public class  NaiveSubgraphMatching3Reader extends AdjacencyListTextVertexReader {
			
			protected String[] preprocessLine(Text line) throws IOException {
				String[] values = SEPARATOR.split(line.toString());
				return values;
			}

			    @Override
			    protected IntWritable getId(String[] values) throws IOException {
			      return decodeId(values[0]);
			    }


			    @Override
			    protected IntWritable getValue(String[] values) throws IOException {
			      return decodeValue(null);
			    }

			    @Override
			    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(String[] values) throws
			        IOException {
			      int i = 1;
			      List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
			      while (i < values.length) {
			        edges.add(decodeEdge(values[i], null));
			        i ++;
			      }
			      return edges;
			    }
			
			@Override
			public IntWritable decodeId(String s) {
				return new IntWritable(Integer.valueOf(s));
			}

			@Override
			public IntWritable decodeValue(String s) {
				return new IntWritable(1);
			}

			@Override
			public Edge<IntWritable, NullWritable> decodeEdge(String id,
					String value) {
				return EdgeFactory.create(decodeId(id), NullWritable.get());
			}
		}
	  } 
	  
	  /**
	   * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
	   */
	  public static class NaiveSubgraphMatching3OutputFormat extends
	      TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new NaiveSubgraphMatching3Writer();
	    }

	    /**
	     * Simple VertexWriter that supports {@link SimplePageRankVertex}
	     */
	    public class NaiveSubgraphMatching3Writer extends TextVertexWriter {
	      @Override
	      public void writeVertex(
	          Vertex<IntWritable, IntWritable, NullWritable, ?> vertex)
	        throws IOException, InterruptedException {
	        getRecordWriter().write(
	            new Text(vertex.getId().toString()),
	            new Text(vertex.getValue().toString()));
	      }
	    }
	  }
}
