/*
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

package org.apache.giraph.partition;

import com.google.common.collect.Lists;
import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Implements loading partitioning schema from partitioned data
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public class IdenticalWithRelabelWorkerPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements WorkerGraphPartitioner<I, V, E, M> {

	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	
	/** Class logger */
	  private static final Logger LOG = Logger.getLogger(
	      IdenticalWithRelabelWorkerPartitioner.class);
  /**
   * Mapping of the vertex ids to {@link PartitionOwner}.
   */
  protected List<PartitionOwner> partitionOwnerList =
      Lists.newArrayList();
	  
  /* map for <oldId, newId> */
  ImmutableClassesGiraphConfiguration conf;
  protected int[] vid2pid = null;
  
  /** partition metadata file path*/
  private String partitionMapPath;
  private int graphVertexNumber;
 
  public IdenticalWithRelabelWorkerPartitioner(ImmutableClassesGiraphConfiguration conf) {
	this.partitionMapPath = conf.get(GiraphConstants.PARTITION_MAP_PATH, GiraphConstants.DEFAULT_PARTITION_MAP_PATH);
	this.graphVertexNumber = conf.getInt(GiraphConstants.GRAPH_VERTEX_NUMBER, GiraphConstants.DEFAULT_GRAPH_VERTEX_NUMBER);
	LOG.info("The partition map path="+this.partitionMapPath+" GraphVertexNumber="+this.graphVertexNumber);
	this.conf = conf;
  }

  public void initialization(Configuration job) {
	Path path = new Path(partitionMapPath); //used to open file by filesystem
	vid2pid = new int[graphVertexNumber];
	int partitionNumber = partitionOwnerList.size();
	int nodeInPartition[] = new int[partitionNumber];
	for(int i = 0; i < partitionNumber; i++){
		nodeInPartition[i] = 0;
	}
	//read the partition file to build the vid2pid map.
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
		  int vid = Integer.valueOf(tokens[0]);
		  int pid = Integer.valueOf(tokens[1]);
		  /* calculate the new id */
		  vid2pid[vid] = nodeInPartition[pid] * partitionNumber + pid;
		  nodeInPartition[pid]++;
      }

    }catch(IOException e) { }
	finally {
      Closeables.closeQuietly(fileIn);
      Closeables.closeQuietly(reader); 
      nodeInPartition = null;
    }
    if(LOG.isDebugEnabled()){
    	for(int i = 0; i < graphVertexNumber; i++)
    		LOG.debug("old_vid=" + i + " ==> new_vid="+vid2pid[i]);
    }
  }

  @Override
  public PartitionOwner createPartitionOwner() {
    return new BasicPartitionOwner();
  }

  @Override
  public PartitionOwner getPartitionOwner(I vertexId) {
//	return partitionOwnerList.get(vid2pid[Integer.valueOf(vertexId.toString())]);
	if(LOG.isDebugEnabled()){
		LOG.debug("vertexId="+vertexId.hashCode());
	}
	return partitionOwnerList.get(Math.abs(vertexId.hashCode()) % partitionOwnerList.size());
  }

  @Override
  public Collection<PartitionStats> finalizePartitionStats(
      Collection<PartitionStats> workerPartitionStats,
      PartitionStore<I, V, E, M> partitionStore) {
    // No modification necessary
    return workerPartitionStats;
  }

  @Override
  public PartitionExchange updatePartitionOwners(
      WorkerInfo myWorkerInfo,
      Collection<? extends PartitionOwner> masterSetPartitionOwners,
      PartitionStore<I, V, E, M> partitionStore) {
    partitionOwnerList.clear();
    partitionOwnerList.addAll(masterSetPartitionOwners);
    
    if(vid2pid == null){
    	this.initialization(conf);
    }

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
  
  public Vertex<I,V,E,M> createNewVeretex(Vertex<I,V,E,M> oldVertex){
	  Vertex<I,V,E,M>  newVertex = conf.createVertex();
	  I newVertexId = (I) new IntWritable(vid2pid[oldVertex.getId().hashCode()]);
	  List<Edge<I, E>> edges = Lists.newLinkedList();
	  for(Edge<I,E> edge: oldVertex.getEdges()){
		  edges.add(EdgeFactory.create((I) new IntWritable(vid2pid[edge.getTargetVertexId().hashCode()]), edge.getValue()));
	  }
	  newVertex.initialize(newVertexId, oldVertex.getValue(), edges);
	  return newVertex;
  }
  
  public int getNewVertexId(I vertexId){
	  return vid2pid[vertexId.hashCode()];
  }
}
