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
import org.apache.giraph.tools.utils.GeneralPartialQueryWritable;
import org.apache.giraph.tools.utils.PartialQueryWritable;
import org.apache.giraph.utils.BitMap;
import org.apache.giraph.utils.EdgeOrientation;
import org.apache.giraph.utils.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class GeneralQuery extends 
Vertex<IntWritable, IntWritable, NullWritable, GeneralPartialQueryWritable> {

	private static final Logger LOG = Logger.getLogger(GeneralQuery.class);
	
	private int label;
	private HashMap<Integer, VertexProperty> node2PropertyMap = null;
	
	@Override
	public void compute(Iterable<GeneralPartialQueryWritable> messages) throws IOException {
			basic_compute(messages);
	}
	
	/**
	 * basic_compute:
	 * initial policy: manually assigned
	 * distribution policy: random
	 * exploration policy: fully enumeration
	 * @param messages
	 */
	private void basic_compute(Iterable<GeneralPartialQueryWritable> messages){
		if(getSuperstep() == 0){
			int initLabel = getInitLabel();
			if(getLabel(getId().get()) == initLabel){
//				LOG.info("QueryGraph:\n"+getQueryGraph().toString());
				int index = 1; //getQueryGraph().getNodeIndexByLabel(initLabel);
//				LOG.info("Send initialized info to vertex: "+getId().get() +" label="+getLabel(getId().get()) +" index="+index);
				if(checkNeighborLabel(getId().get(), getQueryGraph().getNeighborLabel(index))){
					ArrayList<Integer> querySequenceTypeList = getQueryGraphSequence().getQuerySequenceTypeList();
					/* send all query sequence type */
					for(int qst : querySequenceTypeList){
						GeneralPartialQueryWritable tmpPQ = new GeneralPartialQueryWritable(getQueryGraph().getNodeSize(), getQueryGraph().getTotalEdges());
						tmpPQ.setQuerySequenceType(qst);
						tmpPQ.update(index, getId().get());
						tmpPQ.setPreviousMatchedVertexIndex(index);
//						LOG.info("Send PartialQuery:\n"+tmpPQ.toString());
						incSendMessageCount();
						sendMessage(getId(), tmpPQ);		
					}
				}
				else{
					this.incInvalidByUnmatchedVertex();
				}
			}
			voteToHalt();
		}
		else{
			HashMap<Integer, ArrayList<Integer>> queryVertexSet = new HashMap<Integer, ArrayList<Integer>>();
			/* 2. enumerate each partialQuery to generate new partialQuery for the next neighbor. */
			for(GeneralPartialQueryWritable partialQuery : messages){
				incMessageCount();
//				LOG.info("Process partialQuery:\n"+partialQuery.toString());
				int previousMatchedVertexIndex = partialQuery.getPreviousMatchedVertexIndex();
				int qst = partialQuery.getQuerySequenceType();
				EdgeOrientation edgeOrientation = getQueryGraphSequence().getEdgeOrientation(qst);
				ArrayList<Integer> queryGraphEdgeList = getQueryGraph().getEdgeList(previousMatchedVertexIndex);
				
				boolean valid = true;
				for(Integer neighbor : queryGraphEdgeList){
					int index = getQueryGraph().getNodeIndex(neighbor);
					/* 1, if neighbor has been matched, then we need to verify it is ok. 
					 * called Neighbor connection. */
					/* black node*/
					if(partialQuery.isAccessed(getQueryGraph().getNodeIndex(neighbor))){
						continue;
					}
					byte orientation = edgeOrientation.getEdgeOrientation(previousMatchedVertexIndex, index);
					/* gray node */
					if(partialQuery.isMatched(index)){
						valid = checkValid(neighbor, orientation, partialQuery.getMappedDataGraphNode(index));
						if(!valid) 
							this.incInvalidByMatchedVertex();
						partialQuery.decRemainEdges();
					}
					/* White node. 2, if neighbor has not been matched, then we generate query vertex set. */
					else{
						ArrayList<Integer> al = null;
//						LOG.info("QueryType="+qst+": <"+previousMatchedVertexIndex+", "+index+"> ==> orientation="+Integer.valueOf(orientation));
						if(orientation == EdgeOrientation.EDGE_ORIENTATION_BIG) {
							al = getMatchedBigList(neighbor, partialQuery);
						}
						else if(orientation == EdgeOrientation.EDGE_ORIENTATION_SMALL){
							al = getMatchedSmallList(neighbor, partialQuery);
						}
						else {
							al = getMatchedList(neighbor, partialQuery);
						}
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
						if(partialQuery.isCompelete()){
//							if(partialQuery.checkValid())
								incMatchedSubgraphCount();
//							else{
//								System.out.println("Invalid PartialQuery: "+ partialQuery.toString());
//							}
						}
						else{
							/*
							 * Two cases:
							 * 1. exist unmatched vertex
							 * 2. only exist unaccessed vertex
							 * In both cases, we need to send partialQuery to one of unaccessed vertex.
							 */
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
	private void random_distribute_partial_query(
			GeneralPartialQueryWritable partialQuery) {
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
//		do{
			GeneralPartialQueryWritable tempPartialQuery = new GeneralPartialQueryWritable();
			tempPartialQuery.copy(partialQuery);
			tempPartialQuery.setPreviousMatchedVertexIndex(candidateTargets.get(randTarget));
//		LOG.info("Random Send PartialQuery:\n"+tempPartialQuery.toString() 
//				+"\nTarget="+partialQuery.getMappedDataGraphNode(candidateTargets.get(randTarget)));
//			if(checkCandidateNeighborhood(keyArray[nextIdx], matchedDataVertex[nextIdx], tempPartialQuery)){
			/* need to filter by edge index later */
				incSendMessageCount();
				sendMessage(new IntWritable(partialQuery.getMappedDataGraphNode(candidateTargets.get(randTarget))), tempPartialQuery);
//				break;
//			}
//		}while(true);
	}
	
	private void basic_updateAndSendPartialQuery(GeneralPartialQueryWritable pq, HashMap<Integer, ArrayList<Integer>> pr){
		Integer[] queryVertexId = new Integer[pr.keySet().size() + 1];
		Integer[] matchedDataVertex = new Integer[pr.keySet().size() + 1];
		pr.keySet().toArray(queryVertexId);
		recursiveUpdate(pq, queryVertexId, pr, 0, matchedDataVertex);
	}
	
	private void recursiveUpdate(GeneralPartialQueryWritable pq, Integer[] keyArray,
			HashMap<Integer, ArrayList<Integer>> pr, int level, Integer[] matchedDataVertex) {
		if(level >= pr.size()){
			GeneralPartialQueryWritable tempPartialQuery = new GeneralPartialQueryWritable();
			tempPartialQuery.copy(pq);
			int nextIdx;
			do{
				nextIdx = Math.abs(Random.nextInt()) % level;
			}while(nextIdx < 0);
			
			tempPartialQuery.setPreviousMatchedVertexIndex(getQueryGraph().getNodeIndex(keyArray[nextIdx]));
//			LOG.info("Superstep "+getSuperstep()+": "
//			+"Send PartialQuery:\n"+tempPartialQuery.toString()
//			+"\nTargetVertex="+matchedDataVertex[nextIdx]);
//			LOG.info("Send Partial Query: "+tempPartialQuery.toString()
//					+"\nTarget="+matchedDataVertex[nextIdx]);
//			if(checkCandidateNeighborhood(keyArray[nextIdx], matchedDataVertex[nextIdx], tempPartialQuery)){
//			if(checkCandidateNeighborhood(keyArray, matchedDataVertex, level, tempPartialQuery)){
				incSendMessageCount();
				sendMessage(new IntWritable(matchedDataVertex[nextIdx]), tempPartialQuery);
//			}
//			else{
//				/* filter by edge index */
//				incInvalidByMatchedVertexIndex();
//			}
			return ;
		}
		
		Integer key = keyArray[level];
		int index = getQueryGraph().getNodeIndex(key);
		ArrayList<Integer> candidateSet = pr.get(key);
		
		for(Integer cand : candidateSet){
			if(pq.hasUsed(cand)) continue;
			pq.update(index, cand);
			matchedDataVertex[level] = cand;
			/* prune during enumeration */
			if(checkCandidateNeighborhood(key, cand, pq)){
				recursiveUpdate(pq, keyArray, pr, level + 1, matchedDataVertex);
			}
			else{
				incInvalidByMatchedVertexIndex();
			}
			pq.update(index, PartialQueryWritable.PARTIAL_QUERY_NOT_MATCHED);
		}
		return ;
	}

	/**
	 * filter by unmatched
	 * use neighbor label index, degree, label, used
	 * @param queryVertexId
	 * @param partialQuery
	 * @return
	 */
	public ArrayList getMatchedList(int queryVertexId, GeneralPartialQueryWritable partialQuery){
//		LOG.info("In getMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			/* TODO:
			 * how to conveniently access target vertex Id's label? 
			 * each worker needs a global node id -> label index 
			 */
			if(getLabel(edge.getTargetVertexId().get()) == getQueryGraph().getLabel(queryVertexId) 
					&& !partialQuery.hasUsed(edge.getTargetVertexId().get())
					&& getDegree(edge.getTargetVertexId().get()) >= getQueryGraph().getDegree(queryVertexId)
					&& checkNeighborLabel(edge.getTargetVertexId().get(), getQueryGraph().getNeighborLabel(getQueryGraph().getNodeIndex(queryVertexId)))){
//					&& checkCandidateNeighborhood(queryVertexId, edge.getTargetVertexId().get(), partialQuery)){
		
				qualifiedNeighbor.add(edge.getTargetVertexId().get());
			}
		}
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
	public ArrayList getMatchedBigList(int queryVertexId, GeneralPartialQueryWritable partialQuery){
//		LOG.info("In getMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		int curId = getId().get();
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			/* TODO:
			 * how to conveniently access target vertex Id's label? 
			 * each worker needs a global node id -> label index 
			 */
			if(curId > edge.getTargetVertexId().get() &&
					getLabel(edge.getTargetVertexId().get()) == getQueryGraph().getLabel(queryVertexId) 
					&& !partialQuery.hasUsed(edge.getTargetVertexId().get())
					&& getDegree(edge.getTargetVertexId().get()) >= getQueryGraph().getDegree(queryVertexId)
					&& checkNeighborLabel(edge.getTargetVertexId().get(), getQueryGraph().getNeighborLabel(getQueryGraph().getNodeIndex(queryVertexId)))){
//					&& checkCandidateNeighborhood(queryVertexId, edge.getTargetVertexId().get(), partialQuery)){
		
				qualifiedNeighbor.add(edge.getTargetVertexId().get());
			}
		}
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
	public ArrayList getMatchedSmallList(int queryVertexId, GeneralPartialQueryWritable partialQuery){
//		LOG.info("In getMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		int curId = getId().get();
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			/* TODO:
			 * how to conveniently access target vertex Id's label? 
			 * each worker needs a global node id -> label index 
			 */
			if(curId < edge.getTargetVertexId().get() &&
					getLabel(edge.getTargetVertexId().get()) == getQueryGraph().getLabel(queryVertexId) 
					&& !partialQuery.hasUsed(edge.getTargetVertexId().get())
					&& getDegree(edge.getTargetVertexId().get()) >= getQueryGraph().getDegree(queryVertexId)
					&& checkNeighborLabel(edge.getTargetVertexId().get(), getQueryGraph().getNeighborLabel(getQueryGraph().getNodeIndex(queryVertexId)))){
//					&& checkCandidateNeighborhood(queryVertexId, edge.getTargetVertexId().get(), partialQuery)){
		
				qualifiedNeighbor.add(edge.getTargetVertexId().get());
			}
		}
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
	/**
	 * NOTE: each bit in bm must be exist in vid's bitmap.
	 * @param vid
	 * @param bm
	 * @return
	 */
	private boolean checkNeighborLabel(int vid, BitMap bm){
		if(this.node2PropertyMap == null){
			node2PropertyMap = getVertex2PropertyMap();
		}
//		LOG.info("Info in VertexProgram:"+bm.toString());
		return node2PropertyMap.get(vid).checkNeighborLabel(bm);
	}
	
	/**
	 * check the neighborhood structure of unvisited queryVertexId's candidate.
	 * NOTE: only enumerate neighborhood of vertex in query graph, 
	 * and check existence of edge in data graph by edge index.
	 * @param queryVertexId
	 * @param candidate
	 * @return
	 */
	private boolean checkCandidateNeighborhood(int queryVertexId, int candidate, GeneralPartialQueryWritable partialQuery){
		ArrayList<Integer> queryNeighbors = getQueryGraph().getEdgeList(getQueryGraph().getNodeIndex(queryVertexId));
		for(int neighbor : queryNeighbors){
			/*need count here!*/
			int idx = getQueryGraph().getNodeIndex(neighbor);
			if(partialQuery.isMatched(idx) && !partialQuery.isAccessed(idx)){
				if(!checkEdgeExistence(partialQuery.getMappedDataGraphNode(idx), candidate)){
					return false;
				}
			}
		}
		return true;
	}
	
	/**
	 * strong version. 
	 * check each newly mapped vertex.
	 * @param queryVertexId
	 * @param candidate
	 * @param size
	 * @param partialQuery
	 * @return
	 */
	private boolean checkCandidateNeighborhood(Integer[] queryVertexId, Integer[] candidate, int size, GeneralPartialQueryWritable partialQuery){
		for(int i = 0; i < size; i++){
			if(!(checkCandidateNeighborhood(queryVertexId[i], candidate[i], partialQuery))){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Check whether edge exist or not?
	 * inefficient implementation: 
	 * 		need to enumerate vertex's neighborhood in data graph.
	 * @param vid
	 * @return
	 */
	public boolean checkValid(int queryVertexId, byte edgeOrientation, int mappedDataVertexId){
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			if(edge.getTargetVertexId().get() == mappedDataVertexId){
				switch(edgeOrientation){
					case EdgeOrientation.EDGE_ORIENTATION_BIG:
						if(getId().get() < mappedDataVertexId) return false;
						break;
					case EdgeOrientation.EDGE_ORIENTATION_SMALL:
						if(getId().get() > mappedDataVertexId) return false;
						break;
				}
				return true;
			}
		}
		return false;
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
	
	public boolean checkEdgeExistence(long s, long e){
		return getEdgeIndex().checkEdgeExistence(s, e);
	}
	
	
	/******************************
	 * IO format
	 ******************************/
	/** Vertex InputFormat */
	  public static class GeneralQueryInputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, GeneralPartialQueryWritable> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new GeneralQueryReader();
		}
		
		public class  GeneralQueryReader extends AdjacencyListTextVertexReader {
			
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
			        i++;
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
	  public static class GeneralQueryOutputFormat extends
	      TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new GeneralQueryWriter();
	    }

	    /**
	     * Simple VertexWriter that supports {@link SimplePageRankVertex}
	     */
	    public class GeneralQueryWriter extends TextVertexWriter {
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
