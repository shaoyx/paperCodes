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
public class SemiClusterMessage implements
    WritableComparable<SemiClusterMessage>,
    ImmutableClassesGiraphConfigurable  {

  private String semiClusterId;
  private double semiClusterScore;
  private List<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage>> semiClusterVertexList 
  			= new ArrayList<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage>>();
  private Set<SemiClusterDetails> semiClusterContainThis = new TreeSet<SemiClusterDetails>();

  private ImmutableClassesGiraphConfiguration conf;
  
  public SemiClusterMessage(String scId,
      List<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage>> verticesEdges,
      double score) {
    this.semiClusterId = scId;
    this.semiClusterVertexList = verticesEdges;
    this.semiClusterScore = score;
  }

  public SemiClusterMessage(SemiClusterMessage msg) {
    this.semiClusterId = msg.getScId();
    for (Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage> v : msg
        .getVertexList())
      this.semiClusterVertexList.add(v);
    this.semiClusterScore = msg.getScore();
  }

  public SemiClusterMessage(Set<SemiClusterDetails> semiClusterContainThis) {
    this.semiClusterId = "";
    this.semiClusterScore = 0.0;
    this.semiClusterVertexList = null;
    this.semiClusterContainThis = semiClusterContainThis;
  }

  public SemiClusterMessage() {
  }

  public double getScore() {
    return semiClusterScore;
  }

  public void setScore(double score) {
    this.semiClusterScore = score;
  }

  public List<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage>> getVertexList() {
    return semiClusterVertexList;
  }

  public void addVertex(Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage> v) {
    this.semiClusterVertexList.add(v);
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
    if (in.readBoolean()) {
      int len = in.readInt();
      if (len > 0) {
        for (int i = 0; i < len; i++) {
          SemiClusteringVertex v = new SemiClusteringVertex();
          v.setConf(conf); /* pass the configuration to the new vertex. */
          v.readFields(in);
          semiClusterVertexList.add(v);
        }
      }
    }
    int len = in.readInt();
    if (len > 0) {
      for (int i = 0; i < len; i++) {
        SemiClusterDetails sd = new SemiClusterDetails();
        sd.readFields(in);
        semiClusterContainThis.add(sd);
      }
    }

  }

  private void clear() {
    semiClusterVertexList = new ArrayList<Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage>>();
    semiClusterContainThis = new TreeSet<SemiClusterDetails>();
  }

//  public int getSize(){
//	  int res = semiClusterId.length() + Double.SIZE/8; // 1 char = 1 Byte, double = 4B
//	  res += 1;
//	  if(this.semiClusterVertexList != null){
//		  res += 4;
//		  for (Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage> v : semiClusterVertexList) {
//		      v.get  v.write(out);
//		  }
//	  }
//	  return res;
//  }
  
  public void write(DataOutput out) throws IOException {
//	  System.err.println("semiCLusterId="+semiClusterId);
    out.writeUTF(semiClusterId);
    out.writeDouble(semiClusterScore);

    if (this.semiClusterVertexList == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(semiClusterVertexList.size());
      for (Vertex<IntWritable, SemiClusterMessage, DoubleWritable, SemiClusterMessage> v : semiClusterVertexList) {
        v.write(out);
      }
    }
    out.writeInt(semiClusterContainThis.size());
    Iterator<SemiClusterDetails> itr = semiClusterContainThis.iterator();
    while (itr.hasNext())
      itr.next().write(out);
  }

  public Set<SemiClusterDetails> getSemiClusterContainThis() {
    return semiClusterContainThis;
  }

  public void setSemiClusterContainThis(
      List<SemiClusterDetails> semiClusterContainThis,
      int graphJobVertexMaxClusterCount) {
    int clusterCountToBeRemoved = 0;
    NavigableSet<SemiClusterDetails> setSort = new TreeSet<SemiClusterDetails>(
        new Comparator<SemiClusterDetails>() {

          @Override
          public int compare(SemiClusterDetails o1, SemiClusterDetails o2) {
            return (o1.getSemiClusterScore() == o2.getSemiClusterScore() ? 0
                : o1.getSemiClusterScore() < o2.getSemiClusterScore() ? -1 : 1);
          }
        });
    setSort.addAll(this.semiClusterContainThis);
    setSort.addAll(semiClusterContainThis);
    clusterCountToBeRemoved = setSort.size() - graphJobVertexMaxClusterCount;
    Iterator<SemiClusterDetails> itr = setSort.descendingIterator();
    while (clusterCountToBeRemoved > 0) {
      itr.next();
      itr.remove();
      clusterCountToBeRemoved--;
    }
    this.semiClusterContainThis = setSort;

  }

  public int compareTo(SemiClusterMessage m) {
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
    SemiClusterMessage other = (SemiClusterMessage) obj;
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
        + semiClusterScore + ", semiClusterVertexList=" + semiClusterVertexList
        + ", semiClusterContainThis=" + semiClusterContainThis + "]";
  }

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration configuration) {
		conf = configuration;
	}
	
	@Override
	public ImmutableClassesGiraphConfiguration getConf() {
		return conf;
	}
}
