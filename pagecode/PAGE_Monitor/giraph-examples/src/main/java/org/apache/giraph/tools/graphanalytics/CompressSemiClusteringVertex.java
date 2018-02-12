/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.tools.graphanalytics;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.tools.graphanalytics.semiclustering.SemiClusterDetails;
import org.apache.giraph.tools.graphanalytics.semiclustering.SemiClusterMessage;
import org.apache.giraph.tools.graphanalytics.semiclustering.CompressedSemiClusterMessage;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * SemiClusteringVertex Class defines each vertex in a Graph job and the
 * compute() method is the function which is applied on each Vertex in the graph
 * on each Super step of the job execution.
 */
public class CompressSemiClusteringVertex extends
    Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage> {
	
	private final static Logger LOG = Logger.getLogger(CompressSemiClusteringVertex.class); 
  
	private int semiClusterMaximumVertexCount;
	private int graphJobMessageSentCount;
	private int graphJobVertexMaxClusterCount;
	private int superstepLimitation;

	public void setup() {
		semiClusterMaximumVertexCount = getConf().getInt("semicluster.max.vertex.count", 1);
	    graphJobMessageSentCount = getConf().getInt("semicluster.max.message.sent.count", 0);
	    graphJobVertexMaxClusterCount = getConf().getInt("vertex.max.cluster.count", 1);
	    superstepLimitation = getConf().getInt("semicluster.max.supersteps", 20);
	}
  
	/**
	 * The user overrides the Compute() method, which will be executed at each
	 * active vertex in every superstep
	 */
	@Override
	public void compute(Iterable<CompressedSemiClusterMessage> messages) throws IOException {
		if (this.getSuperstep() == 0) {
			setup();
			firstSuperStep();
			voteToHalt();
		}
		if (this.getSuperstep() >= 1 && this.getSuperstep() <= superstepLimitation) {
			Set<CompressedSemiClusterMessage> scListContainThis = new TreeSet<CompressedSemiClusterMessage>();
			Set<CompressedSemiClusterMessage> scListNotContainThis = new TreeSet<CompressedSemiClusterMessage>();
			List<CompressedSemiClusterMessage> scList = new ArrayList<CompressedSemiClusterMessage>();
			for (CompressedSemiClusterMessage msg : messages) {
				this.incMsgCount();
				if (!isVertexInSc(msg)) {
					scListNotContainThis.add(msg);
					CompressedSemiClusterMessage msgNew = new CompressedSemiClusterMessage(msg);
					msgNew.addVertex(this); /* O(d)*/
					msgNew.setScId("C" + createNewSemiClusterName(msgNew.getVertexSet()));
					msgNew.setScore(semiClusterScoreCalcuation(msgNew));
					scListContainThis.add(msgNew);
				} else {
					scListContainThis.add(msg);
				}
			}
			scList.addAll(scListContainThis);
			scList.addAll(scListNotContainThis);
			sendBestSCMsg(scList);
			updatesVertexSemiClustersList(scListContainThis);
		}
		else{
			voteToHalt();
		}
	}

	private int createNewSemiClusterName(Set<Integer> vertexSet) {
	    List<String> vertexIDList = getSemiClusterVerticesIdList(vertexSet);
	    Collections.sort(vertexIDList);
	    return (vertexIDList.hashCode());
	}

	private List<String> getSemiClusterVerticesIdList(Set<Integer> vertexSet) {
		List<String> vertexId = new ArrayList<String>();
		for(Integer vid : vertexSet){
			vertexId.add(vid.toString());
	    }
	    return vertexId;
	}

public List<SemiClusterMessage> addSCList(List<SemiClusterMessage> scList,
      SemiClusterMessage msg) {
    return scList;
  }

  /**
   * This function create a new Semi-cluster ID for a semi-cluster from the list
   * of vertices in the cluster.It first take all the vertexIds as a list sort
   * the list and then find the HashCode of the Sorted List.
   */
  public int createNewSemiClusterName(
      List<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>> semiClusterVertexList) {
    List<String> vertexIDList = getSemiClusterVerticesIdList(semiClusterVertexList);
    Collections.sort(vertexIDList);
    return (vertexIDList.hashCode());
  }

  /**
   * Function which is executed in the first SuperStep
   * 
   * @throws java.io.IOException
   */
  public void firstSuperStep() throws IOException {
	  Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage> v = this;
	  List< Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>> lV =
    		new ArrayList< Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>>();
	  lV.add(v);
	  String newClusterName = "C" + createNewSemiClusterName(lV);
	  /* Transferring vertex as well. */
	  CompressedSemiClusterMessage initialClusters = new CompressedSemiClusterMessage(newClusterName,
        lV, 1);
	  this.sendMessageToAllEdges(initialClusters);
	  Set<SemiClusterDetails> scList = new TreeSet<SemiClusterDetails>();
	  scList.add(new SemiClusterDetails(newClusterName, 1.0));
	  /* the type of value of a vertex is remained. */
	  SemiClusterMessage vertexValue = new SemiClusterMessage(scList);
	  this.setValue(vertexValue);
  }

  /**
   * Vertex V updates its list of semi-clusters with the semi- clusters from c1
   * , ..., ck , c'1 , ..., c'k that contain V
   */
  public void updatesVertexSemiClustersList(
      Set<CompressedSemiClusterMessage> scListContainThis) throws IOException {
    List<SemiClusterDetails> scList = new ArrayList<SemiClusterDetails>();
    Set<CompressedSemiClusterMessage> sortedSet = new TreeSet<CompressedSemiClusterMessage>(
        new Comparator<CompressedSemiClusterMessage>() {

          @Override
          public int compare(CompressedSemiClusterMessage o1, CompressedSemiClusterMessage o2) {
            return (o1.getScore() == o2.getScore() ? 0
                : o1.getScore() < o2.getScore() ? -1 : 1);
          }
        });
    sortedSet.addAll(scListContainThis);
    int count = 0;
    for (CompressedSemiClusterMessage msg : sortedSet) {
      scList.add(new SemiClusterDetails(msg.getScId(), msg.getScore()));
      count++;
      if (count > graphJobMessageSentCount)
        break;
    }

    SemiClusterMessage vertexValue = this.getValue();
    vertexValue
        .setSemiClusterContainThis(scList, graphJobVertexMaxClusterCount);
    this.setValue(vertexValue);
  }

  /**
   * Function to calcualte the Score of a semi-cluster
   * 
   * @param message
   * @return
   */
  public double semiClusterScoreCalcuation(CompressedSemiClusterMessage message) {
    double  fB = 0.0, sC = 0.0;
    int vC = message.getVertexSet().size();
    double bC = message.getbC();
    double iC = message.getiC();
    int eC = message.geteC();
//    List<String> vertexId = getSemiClusterVerticesIdList(message
//        .getVertexList());
//    vC = vertexId.size();
//    for (Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage> v : message
//        .getVertexList()) {
////      List<Edge<IntWritable, DoubleWritable>> eL = v;
//      for (Edge<IntWritable, DoubleWritable> e : v.getEdges()) {
//        eC++;
//        if (vertexId.contains(e.getTargetVertexId().toString())
//            && e.getValue() != null) {
//          iC = iC + e.getValue().get();
//        } else if (e.getValue() != null) {
//          bC = bC + e.getValue().get();
//        }
//      }
//    }
    if (vC > 1)
      sC = ((iC - fB * bC) / ((vC * (vC - 1)) / 2)) / eC;
    return sC;
  }

  /**
   * Returns a Array List of vertexIds from a List of Vertex<Text,
   * DoubleWritable, SCMessage> Objects
   * 
   * @param lV
   * @return
   */
  public List<String> getSemiClusterVerticesIdList(
      List< Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>> lV) {
    Iterator< Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>> vertexItrator = lV
        .iterator();
    List<String> vertexId = new ArrayList<String>();
    while (vertexItrator.hasNext()) {
      vertexId.add(vertexItrator.next().getId().toString());
    }
    return vertexId;
  }

  /**
   * If a semi-cluster c does not already contain V , and Vc < Mmax , then V is
   * added to c to form c' .
   */
  public boolean isVertexInSc(CompressedSemiClusterMessage msg) {
    Set<Integer> vertexId = msg.getVertexSet();
    if (vertexId.contains(this.getId().get())
        || vertexId.size() >= semiClusterMaximumVertexCount)
      return true;
    else
      return false;
  }

  /**
   * The semi-clusters c1 , ..., ck , c'1 , ..., c'k are sorted by their scores,
   * and the best ones are sent to V ?? neighbors.
   */
  public void sendBestSCMsg(List<CompressedSemiClusterMessage> scList) throws IOException {
    Collections.sort(scList, new Comparator<CompressedSemiClusterMessage>() {

      @Override
      public int compare(CompressedSemiClusterMessage o1, CompressedSemiClusterMessage o2) {
        return (o1.getScore() == o2.getScore() ? 0 : o1.getScore() < o2
            .getScore() ? -1 : 1);
      }
    });
    Iterator<CompressedSemiClusterMessage> scItr = scList.iterator();
    int count = 0;
    while (scItr.hasNext()) {
      this.sendMessageToAllEdges(scItr.next());
      count++;
      if (count > graphJobMessageSentCount)
        break;
    }
  }
  
  /** Vertex InputFormat */
	public static class CompressSemiClusteringVertexInputFormat extends
		AdjacencyListTextVertexInputFormat<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage> {
			/** Separator for id and value */
			private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

			@Override
			public AdjacencyListTextVertexReader createVertexReader(
					InputSplit split, TaskAttemptContext context) {
				return new SemiClusteringVertexReader();
			}

			public class  SemiClusteringVertexReader extends AdjacencyListTextVertexReader {
				protected String[] preprocessLine(Text line) throws IOException {
					String[] values = SEPARATOR.split(line.toString());
					return values;
				}

				@Override
				protected IntWritable getId(String[] values) throws IOException {
					return decodeId(values[0]);
				}


				@Override
				protected SemiClusterMessage getValue(String[] values) throws IOException {
					return decodeValue(null);
				}

				@Override
				protected Iterable<Edge<IntWritable, DoubleWritable>> getEdges(String[] values) throws
				IOException {
					int i = 1;
					List<Edge<IntWritable, DoubleWritable>> edges = Lists.newLinkedList();
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
				public SemiClusterMessage decodeValue(String s) {
					SemiClusterMessage scm = new SemiClusterMessage(new TreeSet<SemiClusterDetails>());
					scm.setScId("dumpid");
					return scm;
				}

				@Override
				public Edge<IntWritable, DoubleWritable> decodeEdge(String id,
						String value) {
					return EdgeFactory.create(decodeId(id), new DoubleWritable(1));
				}
			}
		} 

	public static class CompressSemiClusteringVertexOutputFormat extends
		TextVertexOutputFormat<IntWritable, SemiClusterMessage, DoubleWritable> {
			@Override
			public TextVertexWriter createVertexWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
				return new  SemiClusteringVertexWriter();
			}

			/**
			 * Simple VertexWriter that supports {@link BreadthFirstSearch}
			 */
			public class  SemiClusteringVertexWriter extends TextVertexWriter {
				@Override
					public void writeVertex(
							Vertex<IntWritable, SemiClusterMessage, DoubleWritable, ?> vertex)
					throws IOException, InterruptedException {
					StringBuffer strb = new StringBuffer();
					strb.append(vertex.getValue().getSemiClusterContainThis().size()+": ");
					for(SemiClusterDetails scd : vertex.getValue().getSemiClusterContainThis()){
						strb.append("("+scd.getSemiClusterId()+","+String.format("%.3f", scd.getSemiClusterScore())+") ");
					}
					getRecordWriter().write(
							new Text(vertex.getId().toString()),
							new Text(strb.toString()));
							}
			}
		}
}
