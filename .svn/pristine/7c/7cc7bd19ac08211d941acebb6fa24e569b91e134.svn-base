package org.apache.giraph.tools.graphanalytics.relextractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.giraph.comm.messages.VertexMessageStore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.plan.EdgeDirection;
import org.apache.giraph.plan.QueryNode;
import org.apache.giraph.subgraph.graphextraction.PartialAggregatedPath;
import org.apache.hadoop.io.IntWritable;

import com.google.common.collect.Lists;

/**
 * Vertex-centric version.
 * Basic implementation of $\otimes$ operator.
 * -- 0. pre-processing of replicating each core vertex's (in/out) neighbors.
 * -- 1. each core vertex retrieves data from its local message store;
 * -- 2. the vertex merges messages based on $\oplus$ operator
 * -- 3. generate new longer paths and send the paths as messages.
 * 
 * @author yxshao
 *
 */
public class RelExtractionByLineVertex extends Vertex<IntWritable, IntWritable, IntWritable, PartialAggregatedPath> {

	private ArrayList<Integer> inNbrArray;         /* incoming neighbors */
	private ArrayList<Integer> inNbrLabelArray;    /* labels of incoming neighbors */
	private ArrayList<Integer> inWeightArray;      /* incoming edges' weights */
	private ArrayList<Integer> inEdgeLabelArray;   /* incoming edges' labels */
	
	private HashMap<Integer, Integer> outNbrLabel; /* labels of outgoing neighbors */
	
	@Override
	public void compute(Iterable<PartialAggregatedPath> messages) throws IOException {
		if(getSuperstep() == 0) { 
			/* 
			 * preprocess -- send vertex's label, id and edge weight, to its outgoing neighbors
			 *            -- as incoming edges of that neighbor and asking for its
			 *            -- outgoing neighbor's label  
			 */ 
			PartialAggregatedPath msg = new PartialAggregatedPath();
			msg.setVid(this.getId().get());
			msg.setVlabel(this.getValue().get());
			for(Edge<IntWritable, IntWritable> edge : this.getEdges()) {
				msg.setElabel(edge.getValue().get());
				msg.setWeight(1); //fake number here.
				sendMessage(edge.getTargetVertexId(), msg);
				this.incMsgCount();
			}
		}
		else if(getSuperstep() == 1){
			/*
			 * preprocess -- materialize incoming edges (id, vlabel, elabel, weights)
			 *            -- send back the vertex label to incoming edges. 
			 */
			inNbrArray = new ArrayList<Integer>();
			inNbrLabelArray = new ArrayList<Integer>();
			inWeightArray = new ArrayList<Integer>();
			inEdgeLabelArray = new ArrayList<Integer>();
			PartialAggregatedPath newMsg = new PartialAggregatedPath();
			newMsg.setVid(this.getId().get());
			newMsg.setVlabel(this.getValue().get());
			for(PartialAggregatedPath msg : messages) {
				inNbrArray.add(msg.getVid());
				inNbrLabelArray.add(msg.getVlabel());
				inEdgeLabelArray.add(msg.getElabel());
				inWeightArray.add(msg.getWeight());
				sendMessage(new IntWritable(msg.getVid()), newMsg);
				this.incMsgCount();
			}
		}
		else if(getSuperstep() == 2) {
			/*
			 * preprocess -- materialize the outgoing neighbors' label. 
			 */
			outNbrLabel = new HashMap<Integer, Integer> ();
			for(PartialAggregatedPath msg : messages) {
				outNbrLabel.put(msg.getVid(), msg.getVlabel());
			}
			ArrayList<QueryNode> queries = getQueries();
			for(QueryNode query : queries) {
				processQuery(this.getId().get(), 1, query);
			}
		}
		else {
			ArrayList<QueryNode> queries = getQueries();
			if(queries == null) {
				//end the query process and save the results.
				saveNewRelations(messages);
				voteToHalt();
				return ;
			}
			
			VertexMessageStore vms = new VertexMessageStore();
			for(PartialAggregatedPath msg : messages) {
				vms.addMessage(msg.getVid(), msg.getQid(), msg.getWeight()); //organize messages for fast query.
			}
			
			for(QueryNode qn : queries) {
				if(qn.getNodeLabel() == this.getValue().get()) { //same label
					HashMap<Integer, Integer> leftMessages = vms.getMessageByQuery(qn.getLeftQueryId());
					if(leftMessages == null) continue;
							
					for(int sIdx : leftMessages.keySet()) {
						int sWeight = leftMessages.get(sIdx);
						processQuery(sIdx, sWeight, qn);
					}
				}
			}
			vms.clear();
		}
	}
	
	private void saveNewRelations(Iterable<PartialAggregatedPath> messages) {
		HashMap<Integer, Integer> cleanEdges = new HashMap<Integer, Integer>();
		for(PartialAggregatedPath msg : messages) {
//			System.out.println("Final: "+this.getId()+" "+msg.getVid()+" "+msg.getWeight());
			this.incMsgCount();
			int nid = msg.getVid();
			Integer curWeight = cleanEdges.get(nid);
			if(curWeight == null) {
				curWeight = 0;
			}
			curWeight += msg.getWeight(); //aggregation - II;
			cleanEdges.put(nid, curWeight); //TODO: here the root query is Right query, so the messages are grouped by sid.
		}
		
		//save the new relations
		List<Edge<IntWritable, IntWritable>> edges = Lists.newLinkedList();
		for(Integer key : cleanEdges.keySet()){
			edges.add(EdgeFactory.create(new IntWritable(key), new IntWritable(cleanEdges.get(key))));
		}
		this.setEdges(edges);
	}

	public void processQuery(int sid, int sweight, QueryNode qn) {
		if(qn.getNodeLabel() == this.getValue().get()) { //same label
			int qid = qn.getNodeId();
			
			ArrayList<Integer> tid = new ArrayList<Integer>();		
			ArrayList<Integer> tweight = new ArrayList<Integer>();
			
			/* matching process */
			getMatchedEdge(qn.getRightLabel(), qn.getRightEdgeDirection(), qn.getRightEdgeLabel(), tid, tweight);
			PartialAggregatedPath msg =  new PartialAggregatedPath();
			msg.setElabel(qid);
			
			for(int tIdx = 0; tIdx < tid.size(); tIdx++) {
				int tVertexId = tid.get(tIdx);
				msg.setWeight(sweight * tweight.get(tIdx)); //aggregation-I
				int targetId = qn.isLeftQuery() == true ? tVertexId : sid;
				int sourceId = qn.isLeftQuery() == false ? tVertexId : sid;
				msg.setVid(sourceId);
				sendMessage(new IntWritable(targetId), msg);
				this.incMsgCount();
//					System.out.println("Leaf query message: qid="+msg.getQid()+" weight="+msg.getWeight()+" sid="+msg.getVid()+" tid="+targetId+" vid="+this.getId());
			}
		}
	}
	
	private void getMatchedEdge(int vlabel, EdgeDirection edgeDirection,
			int elabel, ArrayList<Integer> sid,
			ArrayList<Integer> sweight) {
		if(edgeDirection == EdgeDirection.INCOMING || edgeDirection == EdgeDirection.BOTH) {
			for(int idx = 0; idx < inNbrArray.size(); ++idx) {
				if((inNbrLabelArray.get(idx) != null && inNbrLabelArray.get(idx) == vlabel) && inEdgeLabelArray.get(idx) == elabel) {
					sid.add(inNbrArray.get(idx));
					sweight.add(inWeightArray.get(idx));
				}
			}
		}

		if(edgeDirection == EdgeDirection.OUTGOING || edgeDirection == EdgeDirection.BOTH) {
			for(Edge<IntWritable, IntWritable> edge : this.getEdges()) {
				if((outNbrLabel.get(edge.getTargetVertexId().get()) != null && outNbrLabel.get(edge.getTargetVertexId().get()) == vlabel) && edge.getValue().get() == elabel) {
					sid.add(edge.getTargetVertexId().get());
					sweight.add(1); //magic number that the initial weight of an edge is 1.
				}
			}
		}
	}

}
