package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

//import junit.framework.Assert;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexProperty;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.tools.utils.GeneralPartialQueryWritableMemOp;
import org.apache.giraph.tools.utils.PartialQueryWritable;
import org.apache.giraph.tools.utils.SkewRandom;
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

public class GeneralQueryNL  extends 
Vertex<IntWritable, IntWritable, NullWritable, GeneralPartialQueryWritableMemOp> {

	@SuppressWarnings("unused")
	private static final Logger LOG = Logger.getLogger(GeneralQueryNL.class);
	
	private HashMap<Integer, VertexProperty> node2PropertyMap = null;
	private int [] bigArray = null;
	private int bigIndex = 0;
	private int [] smallArray = null;
	private int smallIndex = 0;
	int nodeSize;
	int[] grayNodes = null;
	int[][] grayNeighborDist = null; /*0: ALL, 1: small, 2:big*/
	int[] mappedNodes = null;
	int candidateSize = 0;
	int indexCandidate = 0;
	int[][] degree = null;
	
	@Override
	public void compute(Iterable<GeneralPartialQueryWritableMemOp> messages) throws IOException {
			basic_compute(messages);
	}
	
	/**
	 * basic_compute:
	 * initial policy: manually assigned
	 * distribution policy: random
	 * exploration policy: fully enumeration
	 * @param messages
	 */
	@SuppressWarnings("unchecked")
	private void basic_compute(Iterable<GeneralPartialQueryWritableMemOp> messages){
		if(getSuperstep() == 0){
			/* here is the initial work. */
			bigArray = new int[getQueryGraph().getNodeSize()];
			smallArray = new int[getQueryGraph().getNodeSize()];
			bigIndex  = smallIndex = 0;	
			
			/* structure for the distributor */
			nodeSize = getQueryGraph().getNodeSize();
			grayNodes = new int[nodeSize];
			grayNeighborDist = new int[nodeSize][3]; /*0: ALL, 1: small, 2:big*/
			mappedNodes = new int[nodeSize];
			degree = new int[nodeSize][3];
			candidateSize = 0;
			/**
			 * TODO: Initial label choice
			 * Assumption: degree is large, then id is small
			 * Strategy: search from large degree
			 * 		index: 0 ==> small id, large degree ==> costly
			 *      index: 1 ==> large id, small degree ==> cheap
			 */
			int index = getQueryGraph().getNodeIndex(getInitPatternVertex());
//			LOG.info("Initial Pattern Vertex:" + getInitPatternVertex() +" Index: " + index);
			/* send all query sequence type */
			for(int qst : getQueryGraphSequence().getQuerySequenceTypeList()){
				GeneralPartialQueryWritableMemOp tmpPQ = 
						new GeneralPartialQueryWritableMemOp(
								getQueryGraph().getNodeSize(), 
								getQueryGraph().getTotalEdges());
				tmpPQ.setQuerySequenceType(qst);
				tmpPQ.update(index, getId().get());
				tmpPQ.setPreviousMatchedVertexIndex(index);
				incSendMessageCount();
				sendMessage(getId(), tmpPQ);		
			}
		}
		else{
			HashMap<Integer, ArrayList<Integer>> queryVertexSet = new HashMap<Integer, ArrayList<Integer>>();
			/* 2. enumerate each partialQuery to generate new partialQuery for the next neighbor. */
			for(GeneralPartialQueryWritableMemOp partialQuery : messages){
				incMessageCount();
				int previousMatchedVertexIndex = partialQuery.getPreviousMatchedVertexIndex();
				int qst = partialQuery.getQuerySequenceType();
				EdgeOrientation edgeOrientation = getQueryGraphSequence().getEdgeOrientation(qst);
				ArrayList<Integer> queryGraphEdgeList = getQueryGraph().getEdgeList(previousMatchedVertexIndex);
				
				boolean valid = true;
				queryVertexSet.clear();
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
					if(partialQuery.isCompelete()){
						long inc = 1;
						for( ArrayList<Integer> al : queryVertexSet.values()){
							inc = inc * al.size();
						}
						incMatchedSubgraphCount(inc);
					}
					else if(queryVertexSet.size() > 0){
						//TODO: how to handle the situation: when the partialQuery.isCompelete() == true.
						long start_time = System.currentTimeMillis();
						basic_updateAndSendPartialQuery(partialQuery, queryVertexSet);
						incTimeCost(System.currentTimeMillis() - start_time);
					}
					else{
////						LOG.info("Valid Result: "+partialQuery.toString());
//						if(partialQuery.isCompelete()){
//							incMatchedSubgraphCount();
//						}
//						else{
							/*
							 * Two cases:
							 * 1. exist unmatched vertex
							 * 2. only exist unaccessed vertex
							 * In both cases, we need to send partialQuery to one of unaccessed vertex.
							 */
//						    LOG.info("Enter Random Distribution.....");
//						    System.exit(-1);
							random_distribute_partial_query(partialQuery);
//						}
					}
				}
//				queryVertexSet.clear();
			}
			queryVertexSet = null;
		}
		
		/*
		 * After processing current messages, the vertex will vote to halt;
		 */
		voteToHalt();
	}
	
	/**
	 * distribute the partial query to the unmatched node's parent which means trace back.
	 * this process called when the nodes are all gray.
	 * @param partialQuery
	 */
	private void random_distribute_partial_query(
			GeneralPartialQueryWritableMemOp partialQuery) {
//		ArrayList<Integer> candidateTargets = partialQuery.getUnaccessedList();//new ArrayList<Integer>();
//		if(candidateTargets.size() == 0) 
//			return;
//		if(candidateTargets.size() != 2){
//			for(int i = 0; i < candidateTargets.size(); i++){
//				System.out.println("The "+i+"th candidateTargets = "+candidateTargets.get(i));
//			}
//			System.exit(-1);
//		}
		/* here guarantee that all the nodes are GRAY, so 
		 * do need check whether is WHITE here. */
//		LOG.info("In random distribution function.....");
		candidateSize = 0;
		for(int i = 0; i < nodeSize; i++){
			if(!partialQuery.isAccessed(i)){
				grayNodes[candidateSize] = i;
				mappedNodes[candidateSize] = partialQuery.getMappedDataGraphNode(i);
				grayNeighborDist[candidateSize][0] = grayNeighborDist[candidateSize][1] =grayNeighborDist[candidateSize][2] = 0;//for the accurate workload Distributor;
				degree[candidateSize][0] = 1; //for the workload Distributor;
				candidateSize++;
			}
		}
		if(candidateSize == 0){
			return;
		}
		GeneralPartialQueryWritableMemOp tempPartialQuery = new GeneralPartialQueryWritableMemOp();
		tempPartialQuery.copy(partialQuery);
		int nextIdx;
		long start_time = System.currentTimeMillis();
		distributor.setDataVerticesSequence(mappedNodes); //on-line
		distributor.setDegreeSequence(degree); // on-line
		distributor.setQueryCandidateNeighborDist(grayNeighborDist); //once
		nextIdx = distributor.pickTargetId(candidateSize);
		incDistributorTime(System.currentTimeMillis() - start_time);
		tempPartialQuery.setPreviousMatchedVertexIndex(grayNodes[nextIdx]);//getQueryGraph().getNodeIndex(keyArray[nextIdx]));
		incSendMessageCount();
		sendMessage(new IntWritable(mappedNodes[nextIdx]), tempPartialQuery);
		candidateSize = 0; /* maintain invariant */
//		int randTarget;
//		do{
//			randTarget = Math.abs(Random.nextInt()) % candidateTargets.size();
//		}while(randTarget < 0);
//		
//		/** 4. send partialQueryWritable */
//		GeneralPartialQueryWritableMemOp tempPartialQuery = new GeneralPartialQueryWritableMemOp();
//		tempPartialQuery.copy(partialQuery);
//		tempPartialQuery.setPreviousMatchedVertexIndex(candidateTargets.get(randTarget));
//		/* need to filter by edge index later */
//		incSendMessageCount();
//		sendMessage(new IntWritable(partialQuery.getMappedDataGraphNode(candidateTargets.get(randTarget))), tempPartialQuery);
	}
	
	private void basic_updateAndSendPartialQuery(GeneralPartialQueryWritableMemOp pq, HashMap<Integer, ArrayList<Integer>> pr){
		Integer[] queryVertexId = new Integer[pr.keySet().size() + 1];
		int[] matchedDataVertex = new int[pr.keySet().size() + 1];
		pr.keySet().toArray(queryVertexId);
		/* do some precess here! */
		candidateSize = 0;
		indexCandidate = 0;
		boolean isNextNextWhiteNodeExist = false;
		GeneralPartialQueryWritableMemOp tmp = new GeneralPartialQueryWritableMemOp();
		tmp.copy(pq);
		for(int i = 0; i < pr.size(); i++){
			tmp.update(getQueryGraph().getNodeIndex(queryVertexId[i]), 30);/* fake mapped node: 30*/
		}
		
		for(int i = 0; i < nodeSize; i++){
			if(pq.isMatched(i) && !(pq.isAccessed(i))){ // && i != pq.getPreviousMatchedVertexIndex()){ this is guaranteed by the setPreviousMatchedVertex() methods.
//				if(i == pq.getPreviousMatchedVertexIndex()){
//					System.out.println("Conflict for the color of node. Here the expanding vertex should is BLACK, but is GRAY.");
//					System.exit(-1);
//				}
				grayNodes[candidateSize] = i;
				mappedNodes[candidateSize] = pq.getMappedDataGraphNode(i);
				/* get gray neighbor's distribution: 
				 * do this once.
				 * */
				grayNeighborDist[candidateSize][0] = grayNeighborDist[candidateSize][1] = grayNeighborDist[candidateSize][2] = 0;
				isNextNextWhiteNodeExist = false;
				for(int nid : getQueryGraph().getEdgeList(i)){
					/* white node here! */
					if(!tmp.isMatched(getQueryGraph().getNodeIndex(nid))){
						byte idx = getQueryGraphSequence().getEdgeOrientation(pq.getQuerySequenceType()).
								getEdgeOrientation(i, getQueryGraph().getNodeIndex(nid));

//						if(getSuperstep() == 3) {
//							System.out.println("white node before="+idx);
//						}
						if(idx == -1) idx = 0;
						grayNeighborDist[candidateSize][idx]++;
						isNextNextWhiteNodeExist = true;
//						if(getSuperstep() == 3) {
//							System.out.println("grayNeighborDist="+grayNeighborDist[candidateSize][idx]);
//						}
					}
				}
				if(isNextNextWhiteNodeExist){
					degree[candidateSize][0] = getDegree(mappedNodes[candidateSize]);
					degree[candidateSize][1] = getSmallDegree(mappedNodes[candidateSize]);
					degree[candidateSize][2] = getBigDegree(mappedNodes[candidateSize]);
					candidateSize++;
				}
			}
		}
		
		indexCandidate = candidateSize;
		
		/* process new GRAY nodes here. */
		for(int i = 0; i < pr.size(); i++){
			int index = getQueryGraph().getNodeIndex(queryVertexId[i]);
			grayNodes[candidateSize] = index;
			grayNeighborDist[candidateSize][0] = grayNeighborDist[candidateSize][1] = grayNeighborDist[candidateSize][2] = 0;
			isNextNextWhiteNodeExist = false;
			for(int nid : getQueryGraph().getEdgeList(index)){
				/* white node here! */
				if(!tmp.isMatched(getQueryGraph().getNodeIndex(nid))){
					byte idx = getQueryGraphSequence().getEdgeOrientation(tmp.getQuerySequenceType()).
							getEdgeOrientation(index, getQueryGraph().getNodeIndex(nid));

//					if(idx != -1) {
//						System.out.println("Error.............");
//						System.out.println(i+" <==> " + getQueryGraph().getNodeIndex(nid));
//						System.exit(-1);
//					}
//					if(getSuperstep() == 3) {
//						System.out.println("white node="+idx);
//					}
					if(idx == -1) idx = 0;
					grayNeighborDist[candidateSize][idx]++;
					isNextNextWhiteNodeExist = true;
//					if(getSuperstep() == 3) {
//						System.out.println("grayNeighborDist="+grayNeighborDist[candidateSize][idx]);
//					}
				}
			}
			if(isNextNextWhiteNodeExist)
				candidateSize++;
		}
//		if(getSuperstep() ==  2){
//			System.out.println("candidateSize ="+candidateSize+", expected value is 2");
//		}
		recursiveUpdate(pq, queryVertexId, pr, 0, matchedDataVertex);
	}
	
	private void recursiveUpdate(GeneralPartialQueryWritableMemOp pq, Integer[] keyArray,
			HashMap<Integer, ArrayList<Integer>> pr, int level, int[] matchedDataVertex) {
		if(level >= pr.size()){
			if(candidateSize == 0){
				random_distribute_partial_query(pq);
				return;
			}
//			if(candidateSize != indexCandidate){
//				System.out.println("Error: candidate size is not consistent.: candidateSize="+candidateSize+"; indexCandidate="+indexCandidate);
//				System.exit(-1);
//			}
			
			/**
			 * TODO: Do not send the complete partial query. */
			GeneralPartialQueryWritableMemOp tempPartialQuery = new GeneralPartialQueryWritableMemOp();
			tempPartialQuery.copy(pq);
			int nextIdx;
			long start_time = System.currentTimeMillis();
			distributor.setDataVerticesSequence(mappedNodes); //on-line
			distributor.setDegreeSequence(degree); // on-line
			distributor.setQueryCandidateNeighborDist(grayNeighborDist); //once
			nextIdx = distributor.pickTargetId(candidateSize);
			incDistributorTime(System.currentTimeMillis() - start_time);
//			double[] distribution = new double[level];
//			double totalDegree = 0;
//			for(int i = 0; i < level; i++){
//				distribution[i] = getDegree(matchedDataVertex[i]);
//				totalDegree += 1.0 / distribution[i];
//			}
			/* should not sort here. otherwise it destroy the degree sequence. */
//			Arrays.sort(distribution);
//			Collections.sort(new ArrayList(distribution));
//			nextIdx = SkewRandom.randomPick(distribution, totalDegree);
			
//			LOG.info(tempPartialQuery.toString());
//			LOG.info("Target Id: "+ matchedDataVertex[nextIdx]+", degree="+getDegree(matchedDataVertex[nextIdx]));
			tempPartialQuery.setPreviousMatchedVertexIndex(grayNodes[nextIdx]);//getQueryGraph().getNodeIndex(keyArray[nextIdx]));
			incSendMessageCount();
//			sendMessage(new IntWritable(matchedDataVertex[nextIdx]), tempPartialQuery);
			sendMessage(new IntWritable(mappedNodes[nextIdx]), tempPartialQuery);
			return ;
		}
		
		Integer key = keyArray[level];
		int index = getQueryGraph().getNodeIndex(key);
		boolean increase = (grayNodes[indexCandidate] == index);
		ArrayList<Integer> candidateSet = pr.get(key);
		
		for(Integer cand : candidateSet){
			if(pq.hasUsed(cand)) continue;
			pq.update(index, cand);
			matchedDataVertex[level] = cand;
			/* TODO: sequence of the filter
			 * prune during enumeration */
			/* TODO: In fact here we only need to check the orientation between candidates. */
			if(checkOrientation(key, cand, pq) 
				&& checkCandidateNeighborhood(key, cand, pq) ){
				/**
				 * Ensure the same sequence with the preprocess.
				 */
				if(increase){
					mappedNodes[indexCandidate] = cand;
					degree[indexCandidate][0] = getDegree(cand);
					degree[indexCandidate][1] = getSmallDegree(cand);
					degree[indexCandidate][2] = getBigDegree(cand);
					indexCandidate++;
				}
				recursiveUpdate(pq, keyArray, pr, level + 1, matchedDataVertex);
				if(increase){
					indexCandidate--;
				}
			}
//			else{
//				incInvalidByMatchedVertexIndex();
//			}
			pq.update(index, PartialQueryWritable.PARTIAL_QUERY_NOT_MATCHED);
		}
		return ;
	}

	private boolean checkOrientation(Integer key, Integer cand,
			GeneralPartialQueryWritableMemOp pq) {
		long startTime = System.currentTimeMillis();
		EdgeOrientation eo = getQueryGraphSequence().getEdgeOrientation(pq.getQuerySequenceType());
		int index;
		int edgeIndexKey = eo.getVertexIndex(key);
		int edgeIndexVid;
		
		for(int vid : getQueryGraph().getNodeSet()){
			index  = getQueryGraph().getNodeIndex(vid);
			if(pq.isMatched(index) == false) 
				continue;
			edgeIndexVid = eo.getVertexIndex(vid);
//			LOG.info("VertexId="+vid+", candidate="+cand +", matchedVertex="+pq.getMappedDataGraphNode(index));
			byte orientation = eo.getEdgeOrientation(edgeIndexKey, edgeIndexVid);
			if( (orientation == EdgeOrientation.EDGE_ORIENTATION_SMALL && cand > pq.getMappedDataGraphNode(index))
					|| (orientation == EdgeOrientation.EDGE_ORIENTATION_BIG && cand < pq.getMappedDataGraphNode(index))){
				incCheckOrientationCost(System.currentTimeMillis() - startTime);
				incInvalidByOrientation();
				return false;
			}
		}
		incCheckOrientationCost(System.currentTimeMillis() - startTime);
		return true;
	}

	/**
	 * filter by unmatched
	 * use neighbor label index, degree, label, used
	 * @param queryVertexId
	 * @param partialQuery
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public ArrayList getMatchedList(int queryVertexId, GeneralPartialQueryWritableMemOp partialQuery){
		long startTime = System.currentTimeMillis();
//		LOG.info("In getMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		
		initialFilterArray(queryVertexId, partialQuery);
		
		for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
			int targetId = edge.getTargetVertexId().get();
			
			if(checkConstrains(targetId)
				&& checkCandidateNeighborhood(queryVertexId, targetId, partialQuery)
				&& !partialQuery.hasUsed(targetId)
				&& getDegree(targetId) >= getQueryGraph().getDegree(queryVertexId)){
				qualifiedNeighbor.add(targetId);
			}
		}
//		resetFilterArray();
		incGetCandidateCost(System.currentTimeMillis() - startTime);
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
//	private void resetFilterArray() {
//		smallIndex = bigIndex = 0;
//	}

	private boolean checkConstrains(int targetId) {
//		LOG.info("curId: "+getId().get()+" Target Id: "+ targetId);
		for(int i = 0; i < smallIndex; i++){
//			LOG.info("small value: " + smallArray[i]);
			if(smallArray[i] > targetId){
				return false;
			}
		}
		for(int i = 0; i < bigIndex; i++){
//			LOG.info("big value: " + bigArray[i]);
			if(bigArray[i] < targetId){
				return false;
			}
		}
//		LOG.info("return true");
		return true;
	}

	private void initialFilterArray(int queryVertexId, GeneralPartialQueryWritableMemOp partialQuery) {
		EdgeOrientation eo = getQueryGraphSequence().getEdgeOrientation(partialQuery.getQuerySequenceType());
		int index;
		int edgeIndexKey = eo.getVertexIndex(queryVertexId);
		int edgeIndexVid;
		
		smallIndex = bigIndex = 0;
		for(int vid : getQueryGraph().getNodeSet()){
			index  = getQueryGraph().getNodeIndex(vid);
			if(partialQuery.isMatched(index) == false) 
				continue;
			edgeIndexVid = eo.getVertexIndex(vid);
			byte orientation = eo.getEdgeOrientation(edgeIndexKey, edgeIndexVid);
			if(orientation == EdgeOrientation.EDGE_ORIENTATION_SMALL){
				/**
				 * here the queryVertexId should be smaller than vid,
				 * so the vid's map should be bigger than queryVertexId's map(candidate)
				 */
				bigArray[bigIndex++] = partialQuery.getMappedDataGraphNode(index);
			}
			else if(orientation == EdgeOrientation.EDGE_ORIENTATION_BIG){
				smallArray[smallIndex++] = partialQuery.getMappedDataGraphNode(index);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public ArrayList getMatchedBigList(int queryVertexId, GeneralPartialQueryWritableMemOp partialQuery){
		long startTime = System.currentTimeMillis();
//		LOG.info("In getBigMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		
		initialFilterArray(queryVertexId, partialQuery);
			
		for(Edge<IntWritable, NullWritable> edge : this.getBigEdges()){
			int targetId = edge.getTargetVertexId().get(); /* the candidate */
			if(checkConstrains(targetId)
				&& checkCandidateNeighborhood(queryVertexId, targetId, partialQuery)
				&& !partialQuery.hasUsed(targetId)
				&& getDegree(targetId) >= getQueryGraph().getDegree(queryVertexId)){
				qualifiedNeighbor.add(targetId);
			}
		}
//		resetFilterArray();
		incGetCandidateCost(System.currentTimeMillis() - startTime);
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
	@SuppressWarnings("rawtypes")
	public ArrayList getMatchedSmallList(int queryVertexId, GeneralPartialQueryWritableMemOp partialQuery){
		long startTime = System.currentTimeMillis();
//		LOG.info("In getSmallMatchedList queryVertexId="+queryVertexId);
		ArrayList<Integer> qualifiedNeighbor = new ArrayList<Integer>();
		
		initialFilterArray(queryVertexId, partialQuery);
		
		for(Edge<IntWritable, NullWritable> edge : this.getSmallEdges()){
			int targetId = edge.getTargetVertexId().get();
			if(checkConstrains(targetId)
				&& checkCandidateNeighborhood(queryVertexId, targetId, partialQuery)
				&& !partialQuery.hasUsed(targetId)
				&& getDegree(targetId) >= getQueryGraph().getDegree(queryVertexId)){
				qualifiedNeighbor.add(targetId);
			}
		}
//		resetFilterArray();
		incGetCandidateCost(System.currentTimeMillis() - startTime);
		return (qualifiedNeighbor.size() == 0 ? null : qualifiedNeighbor);
	}
	
	/**
	 * check the neighborhood structure of unvisited queryVertexId's candidate.
	 * NOTE: only enumerate neighborhood of vertex in query graph, 
	 * and check existence of edge in data graph by edge index.
	 * @param queryVertexId
	 * @param candidate
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean checkCandidateNeighborhood(int queryVertexId, int candidate, GeneralPartialQueryWritableMemOp partialQuery){
		long startTime = System.currentTimeMillis();
		ArrayList<Integer> queryNeighbors = getQueryGraph().getEdgeList(getQueryGraph().getNodeIndex(queryVertexId));
		for(int neighbor : queryNeighbors){
			/*need count here!*/
			int idx = getQueryGraph().getNodeIndex(neighbor);
			if(partialQuery.isMatched(idx) && !partialQuery.isAccessed(idx)){
				/**
				 * TODO: 
				 * if we want to access the candidate's neighbor hood, 
				 * the system should support graph storage.
				 */
				if(!checkEdgeExistence(partialQuery.getMappedDataGraphNode(idx), candidate)){
					incCheckCandidateNeighborhoodCost(System.currentTimeMillis() - startTime);
					incInvalidByMatchedVertexIndex();
					return false;
				}
			}
		}
		incCheckCandidateNeighborhoodCost(System.currentTimeMillis() - startTime);
		return true;
	}
	
	/**
	 * Check whether edge exist or not?
	 * enumerate according to edge orientation.
	 * TODO: using BinarySearch with sorted neighbor list.
	 * @param vid
	 * @return
	 */
	public boolean checkValid(int queryVertexId, byte edgeOrientation, int mappedDataVertexId){
		long startTime = System.currentTimeMillis();
		
		if(isTargetIdExist(mappedDataVertexId)){
			incCheckValidCost(System.currentTimeMillis() - startTime);
			return true;
		}
		else{
			incCheckValidCost(System.currentTimeMillis() - startTime);
			return false;
		}
		
//		Iterable<Edge<IntWritable, NullWritable>> edges;
//		if(EdgeOrientation.EDGE_ORIENTATION_BIG == edgeOrientation){
//			edges = getBigEdges();
//		}
//		else if(EdgeOrientation.EDGE_ORIENTATION_SMALL == edgeOrientation){
//			edges = getSmallEdges();
//		}
//		else{
//			edges = getEdges();
//		}
//		
//		/**
//		 * TODO: can be optimized by BinarySearch
//		 */
//		for(Edge<IntWritable, NullWritable> edge : edges){
//			if(edge.getTargetVertexId().get() == mappedDataVertexId){		
//				incCheckValidCost(System.currentTimeMillis() - startTime);
//				return true;
//			}
//		}
//		incCheckValidCost(System.currentTimeMillis() - startTime);
//		return false;
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
	
	public int getSmallDegree(int vid){
		if(this.node2PropertyMap == null){
			node2PropertyMap = getVertex2PropertyMap();
		}
		return node2PropertyMap.get(vid).getSmallDegree();
	}
	
	public int getBigDegree(int vid){
		if(this.node2PropertyMap == null){
			node2PropertyMap = getVertex2PropertyMap();
		}
		return node2PropertyMap.get(vid).getBigDegree();
	}
	
	public boolean checkEdgeExistence(long s, long e){
//		if(getSuperstep() == 2){
//			System.err.println("\t"+(s>e?e:s)+" " + (s>e?s:e));
//		}
		return getEdgeIndex().checkEdgeExistence(s, e);
	}
	
	
	/******************************
	 * IO format
	 ******************************/
	/** Vertex InputFormat */
	  public static class GeneralQueryNLInputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, GeneralPartialQueryWritableMemOp> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new GeneralQueryNLReader();
		}
		
		public class  GeneralQueryNLReader extends AdjacencyListTextVertexReader {
			
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
	  public static class GeneralQueryNLOutputFormat extends
	      TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new GeneralQueryNLWriter();
	    }

	    /**
	     * Simple VertexWriter that supports {@link SimplePageRankVertex}
	     */
	    public class GeneralQueryNLWriter extends TextVertexWriter {
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
