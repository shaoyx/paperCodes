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

package org.apache.giraph.tools.graphanalytics.semiclustering;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.tools.graphanalytics.SemiClusteringVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * The SemiClusterMessage class defines the structure of the value stored by
 * each vertex in the graph Job which is same as the Message sent my each
 * vertex.
 * 
 */
public class CompressedSemiClusterMessage implements
    WritableComparable<CompressedSemiClusterMessage>
//,    ImmutableClassesGiraphConfigurable  {
	{

  private String semiClusterId;
  private double semiClusterScore;
  
  private double iC; //the weight of internal edges.
  private double bC; //the weight of cut edges.
  private int eC;    //edge count. Internal edge count twice.
 
  private Set<Integer> vertexSet = new TreeSet<Integer>(); //record the vertices in this cluster.

//  private ImmutableClassesGiraphConfiguration conf;
  
  public CompressedSemiClusterMessage(String scId,
      List<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>> verticesEdges,
      double score) {
    this.semiClusterId = scId;
    this.semiClusterScore = score;
    this.iC = 0.0;
    this.bC = 0.0;
    this.eC = 0;
    this.addVertexList(verticesEdges);
  }

  public CompressedSemiClusterMessage(CompressedSemiClusterMessage msg) {
    this.semiClusterId = msg.getScId();
    this.semiClusterScore = msg.getScore();
    this.iC = msg.getiC();
    this.bC = msg.getbC();
    this.eC = msg.geteC();
    for (Integer v : msg.getVertexSet())
      this.vertexSet.add(v);
  }

  public CompressedSemiClusterMessage() {
  }

  public double getScore() {
    return semiClusterScore;
  }

  public void setScore(double score) {
    this.semiClusterScore = score;
  }
  
  public double getiC() {
	  return this.iC;
  }
  
  public void setiC(double iC) {
	  this.iC = iC;
  }
  
  public double getbC(){
	  return this.bC;
  }
  
  public void setbC(double bC){
	  this.bC = bC;
  }
  
  public int geteC(){
	  return this.eC;
  }
  
  public void seteC(int eC){
	  this.eC = eC;
  }

  public Set<Integer> getVertexSet() {
    return vertexSet;
  }

  public void addVertexList(List<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage>> VL){
	  for(Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage> v : VL){
		  this.addVertex(v);
	  }
  }
  
  /**
   * Update the aggregated values altogether.
   * @param v
   */
  public void addVertex(Vertex<IntWritable, SemiClusterMessage, DoubleWritable, CompressedSemiClusterMessage> v) {
      for (Edge<IntWritable, DoubleWritable> e : v.getEdges()) {
    	  eC++;
	      if (vertexSet.contains(e.getTargetVertexId().get())
	            && e.getValue() != null) {
	          iC = iC + 2*e.getValue().get();
	          bC = bC - e.getValue().get();
	        } else if (e.getValue() != null) {
	          bC = bC + e.getValue().get();
	        }
      }
      vertexSet.add(v.getId().get());
  }

  public String getScId() {
    return semiClusterId;
  }

  public void setScId(String scId) {
    this.semiClusterId = scId;
  }

  public void readFields(DataInput in) throws IOException {
    clear();
    String semiClusterId = in.readUTF();
    setScId(semiClusterId);
    double score = in.readDouble();
    setScore(score);
    
    setiC(in.readDouble());
    setbC(in.readDouble());
    seteC(in.readInt());
    
    int len = in.readInt();
    if (len > 0) {
    	for (int i = 0; i < len; i++) {
          int vid = in.readInt();
          vertexSet.add(vid);
        }
    }
  }

  private void clear() {
	vertexSet = new TreeSet<Integer>();
  }
  
  public void write(DataOutput out) throws IOException {
//	  System.err.println("semiCLusterId="+semiClusterId);
    out.writeUTF(semiClusterId);
    out.writeDouble(semiClusterScore);
    
    out.writeDouble(this.iC);
    out.writeDouble(this.bC);
    out.writeInt(this.eC);

    int size = vertexSet.size();
    out.writeInt(size);
    if(size > 0){
      for (Integer vid : vertexSet) {
        out.writeInt(vid);
      }
    }
  }
  
  public int compareTo(CompressedSemiClusterMessage m) {
    return (this.getScId().compareTo(m.getScId()));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((semiClusterId == null) ? 0 : semiClusterId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CompressedSemiClusterMessage other = (CompressedSemiClusterMessage) obj;
    if (semiClusterId == null) {
      if (other.semiClusterId != null)
        return false;
    } else if (!semiClusterId.equals(other.semiClusterId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SCMessage [semiClusterId=" + semiClusterId + ", semiClusterScore="
        + semiClusterScore + ", iC=" + iC+" bC="+bC+" eC="+eC
        + ", vertexSet=" + vertexSet.toString() + "]";
  }

//	@Override
//	public void setConf(ImmutableClassesGiraphConfiguration configuration) {
//		conf = configuration;
//	}
//	
//	@Override
//	public ImmutableClassesGiraphConfiguration getConf() {
//		return conf;
//	}
}
