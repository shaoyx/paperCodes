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
import org.apache.giraph.tools.utils.BitMapWritable;
import org.apache.giraph.tools.utils.PartialQueryWritable2;
import org.apache.giraph.utils.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class NaiveSubgraphMatching2 extends 
Vertex<IntWritable, IntWritable, NullWritable, PartialQueryWritable2> {

	private static final Logger LOG = Logger.getLogger(NaiveSubgraphMatching2.class);
	
	private int label;
	private HashMap<Integer, VertexProperty> node2PropertyMap = null;

	@Override
	public void compute(Iterable<PartialQueryWritable2> messages) throws IOException {
		explore_one_neighbor_compute(messages);
	
	}

	/**
	 * explore_one_neighbor_compute:
	 * initial policy: manually assigned
	 * distribution policy: random
	 * exploration policy: random choose one neighbor
	 * TODO: bug: can not handle situation when nodes a not connect where DFS needs to traceback.
	 * @param messages
	 */
	private void explore_one_neighbor_compute(Iterable<PartialQueryWritable2> messages){
		if(getSuperstep() == 0){
			int initLabel = getInitLabel();
			if(getLabel(getId().get()) == initLabel){
				int index = getQueryGraph().getNodeIndexByLabel(initLabel);
				PartialQueryWritable2 tmpPQ = new PartialQueryWritable2(getQueryGraph().getNodeSize(), getQueryGraph().getTotalEdges());
				tmpPQ.update(index, getId().get());
				tmpPQ.setPreviousMatchedVertexIndex(index);
//				LOG.info("Send PartialQuery:\n"+tmpPQ.toString());
				sendMessage(getId(), tmpPQ);						
			}
			voteToHalt();
		}
		else{
			HashMap<Integer, ArrayList<Integer>> queryVertexSet = new HashMap<Integer, ArrayList<Integer>>();
	//		/* 2. enumerate each partialQuery to generate new partialQuery for the next neighbor. */
			for(PartialQueryWritable2 partialQuery : messages){
				incMessageCount();

//				LOG.info("Processing PartialQuery:\n"+partialQuery.toString());
				/* Expand1: traversal graph */
				int previousMatchedVertexIndex = partialQuery.getPreviousMatchedVertexIndex();
				ArrayList<Integer> queryGraphEdgeList = getQueryGraph().getEdgeList(previousMatchedVertexIndex);
				boolean valid = true;
				/* construct query set */
				for(Integer neighbor : queryGraphEdgeList){
					int index = getQueryGraph().getNodeIndex(neighbor);
					/* 1, if neighbor has been matched, then we need to verify it is ok. 
					 * called Neighbor connection. */
					if(partialQuery.isMatched(index)){
						valid = checkValid(neighbor, partialQuery.getMappedDataGraphNode(index));
						if(!valid) this.incInvalidByMatchedVertex();
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
							partialQuery.decRemainUnAccessedEdge();
						}
					}					
					if(!valid) break;
				}
				if(valid){
					/* generate new partial query. */
					if(queryVertexSet.size() > 0){
						explore_one_neighbor_updateAndSendPartialQuery(partialQuery, queryVertexSet);
					}
					else{
//						LOG.info("Valid Result: "+partialQuery.toString());
						if(partialQuery.isCompelete()){
							/* enumerate all valid partial query here*/
//							partialQuery.getTotalCombination();
							incMatchedSubgraphCount(partialQuery.getTotalCombination());
						}
						else{
							/* need to random pick a remained node and do traversal */
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

	private void random_distribute_partial_query(
			PartialQueryWritable2 partialQuery) {
		int idx = partialQuery.getUnMatched();
		BitMapWritable bmw = partialQuery.getCandidate(idx);
		partialQuery.removeCandidate(idx);
		ArrayList<Integer> neighborList = bmw.getIndexList();
		PartialQueryWritable2 tempPartialQuery;
		
		/* 4. send partialQueryWritable */
		for(int target : neighborList){
			tempPartialQuery = new PartialQueryWritable2();
			tempPartialQuery.copy(partialQuery);
			tempPartialQuery.update(idx, target);
			tempPartialQuery.setPreviousMatchedVertexIndex(idx);
//			LOG.info("Random Send PartialQuery:\n"+tempPartialQuery.toString() +"\nTarget="+target);
			incSendMessageCount();
			sendMessage(new IntWritable(target), tempPartialQuery);
		}
	}

	private void explore_one_neighbor_updateAndSendPartialQuery(PartialQueryWritable2 pq, HashMap<Integer, ArrayList<Integer>> queryset){
		int neighborIdx;
		int query_set_size = queryset.size();
		
		/* 1. construct related neighbor's new candidate list */
		HashMap<Integer, BitMapWritable> candidateList = new HashMap<Integer, BitMapWritable>();
		for(int key : queryset.keySet()){
//			LOG.info("queryset: key="+key);
			BitMapWritable bmw = pq.getCandidate(getQueryGraph().getNodeIndex(key));
			BitMapWritable new_bmw = new BitMapWritable();
			ArrayList<Integer> al = queryset.get(key);
			if(bmw == null){
				for(int neighbor : al){
//					LOG.info("\t neighbor="+neighbor);
					new_bmw.set(neighbor);
				}
			}
			else{
				for(int i = 0; i < al.size(); ++i){
//					LOG.info("\t neighbor="+al.get(i));
					if(bmw.get(al.get(i)))
						new_bmw.set(al.get(i));
					else{
						al.remove(i);
					}
				}
			}
			if(al.size() == 0){
				if(pq.isCompelete() == false){
					/* random pick a node here */
					random_distribute_partial_query(pq);
				}
				return ;
			}
			pq.updateCandidate(getQueryGraph().getNodeIndex(key), new_bmw);
			candidateList.put(key, new_bmw);
			bmw = null;
		}
		
		if(pq.isCompelete()){
			incMatchedSubgraphCount(pq.getTotalCombination());
			return;
		}
		
		/* 2. random pick a neighbor */
		do{
			neighborIdx = Math.abs(Random.nextInt()) % query_set_size;
		}while(neighborIdx < 0);
		
		/* 3. ready for sending partialQueryWritable */
		PartialQueryWritable2 tempPartialQuery;
		Integer [] keyArray = new Integer[query_set_size+1];
		queryset.keySet().toArray(keyArray);
		ArrayList<Integer> targets = queryset.get(keyArray[neighborIdx]);
		
//		for(int idx = 0; idx < keyArray.length - 1; ++idx){
//			if(idx == neighborIdx){
//				pq.removeCandidate(getQueryGraph().getNodeIndex(keyArray[idx]));
//			}
//			else{
//				pq.updateCandidate(getQueryGraph().getNodeIndex(keyArray[idx]), candidateList.get(keyArray[idx]));
//			}
//		}
		
		pq.removeCandidate(getQueryGraph().getNodeIndex(keyArray[neighborIdx]));
		
//		LOG.info("PreviousMatchedKey="+keyArray[neighborIdx] +" PreviousMatchedKeyIndex="+getQueryGraph().getNodeIndex(keyArray[neighborIdx]) +" index="+neighborIdx);
		
		/* 4. send partialQueryWritable */
		for(int target : targets){
			tempPartialQuery = new PartialQueryWritable2();
			tempPartialQuery.copy(pq);
			tempPartialQuery.update(getQueryGraph().getNodeIndex(keyArray[neighborIdx]), target);
			tempPartialQuery.setPreviousMatchedVertexIndex(getQueryGraph().getNodeIndex(keyArray[neighborIdx]));
//			LOG.info("Send PartialQuery:\n"+tempPartialQuery.toString()+"\nTarget="+target);
			incSendMessageCount();
			sendMessage(new IntWritable(target), tempPartialQuery);
		}
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
	
	public ArrayList getMatchedList(int queryVertexId, PartialQueryWritable2 partialQuery){
		
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
	 * inefficient implementation.
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
	  public static class NaiveSubgraphMatching2InputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, PartialQueryWritable2> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	
		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new NaiveSubgraphMatching2Reader();
		}
		
		public class  NaiveSubgraphMatching2Reader extends AdjacencyListTextVertexReader {
			
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
	  public static class NaiveSubgraphMatching2OutputFormat extends
	      TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new NaiveSubgraphMatching2Writer();
	    }
	
	    /**
	     * Simple VertexWriter that supports {@link SimplePageRankVertex}
	     */
	    public class NaiveSubgraphMatching2Writer extends TextVertexWriter {
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