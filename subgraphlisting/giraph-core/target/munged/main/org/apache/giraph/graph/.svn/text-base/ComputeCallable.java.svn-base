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
package org.apache.giraph.graph;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.metrics.TimerDesc;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionContext;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.giraph.time.Times;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.TimedLogger;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerThreadAggregatorUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compute as many vertex partitions as possible.  Every thread will has its
 * own instance of WorkerClientRequestProcessor to send requests.  Note that
 * the partition ids are used in the partitionIdQueue rather than the actual
 * partitions since that would cause the partitions to be loaded into memory
 * when using the out-of-core graph partition store.  We should only load on
 * demand.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class ComputeCallable<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable> implements Callable {
  /** Class logger */
  private static final Logger LOG  = Logger.getLogger(ComputeCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.get();
  /** Context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state (note that it is recreated in call() for locality) */
  private GraphState<I, V, E, M> graphState;
  /** Thread-safe queue of all partition ids */
  private final BlockingQueue<Integer> partitionIdQueue;
  /** Message store */
  private final MessageStoreByPartition<I, M> messageStore;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** Worker (for NettyWorkerClientRequestProcessor) */
  private final CentralizedServiceWorker<I, V, E, M> serviceWorker;
  /** Dump some progress every 30 seconds */
  private final TimedLogger timedLogger = new TimedLogger(30 * 1000, LOG);
  /** Sends the messages (unique per Callable) */
  private WorkerClientRequestProcessor<I, V, E, M>
  workerClientRequestProcessor;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();

  // Per-Superstep Metrics
  /** Messages sent */
  private final Counter messagesSentCounter;
  /** Timer for single compute() call */
  private final Timer computeOneTimer;

  private long totalMsgCount = 0;
  private long maxMsgProcessByVertex = 0;
  private long maxMsgProcessVertexId = -1;
  private long maxMsgGenerationByVertex = 0;
  private long maxMsgGenerationVertexId = -1;
  private long totalSendMsgCount = 0;
  private long totalInvalidByMatchedVertex = 0;
  private long totalInvalidByMatchedVertexIndex = 0;
  private long totalInvalidByUnmatchedVertex = 0;
  private long totalTimeCost = 0;
  private long totalCheckValidCost = 0;
  private long totalCheckOrientationCost = 0;
  private long totalGetCandidateCost = 0;
  private long totalCheckCandidateNeighborhoodCost = 0;
  private long totalInvalidByOrientation = 0;
  private long realTimeCost = 0;
  private long distributorCost = 0;

//  	private AtomicInteger totalMsgCount = new AtomicInteger();
//	private AtomicInteger totalSendMsgCount = new AtomicInteger();
//	private AtomicInteger totalInvalidByMatchedVertex = new AtomicInteger();
//	private AtomicInteger totalInvalidByUnmatchedVertex = new AtomicInteger();
  Distributor distributor;
  /**
   * Constructor
   *
   * @param context Context
   * @param graphState Current graph state (use to create own graph state)
   * @param messageStore Message store
   * @param partitionIdQueue Queue of partition ids (thread-safe)
   * @param configuration Configuration
   * @param serviceWorker Service worker
   */
  public ComputeCallable(
      Mapper<?, ?, ?, ?>.Context context, GraphState<I, V, E, M> graphState,
      MessageStoreByPartition<I, M> messageStore,
      BlockingQueue<Integer> partitionIdQueue,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      CentralizedServiceWorker<I, V, E, M> serviceWorker) {
    this.context = context;
    this.configuration = configuration;
    this.partitionIdQueue = partitionIdQueue;
    this.messageStore = messageStore;
    this.serviceWorker = serviceWorker;
    // Will be replaced later in call() for locality
    this.graphState = graphState;

    SuperstepMetricsRegistry metrics = GiraphMetrics.get().perSuperstep();
    // Normally we would use ResetSuperstepMetricsObserver but this class is
    // not long-lived, so just instantiating in the constructor is good enough.
    computeOneTimer = metrics.getTimer(TimerDesc.COMPUTE_ONE);
    messagesSentCounter = metrics.getCounter(MetricNames.MESSAGES_SENT);
    
    /* create the distributor here! */
    String distributorType = configuration.get(GiraphConstants.DISTRIBUTOR_TYPE, 
    		GiraphConstants.DEFAULT_DISTRIBUTOR_TYPE);
    if("random".equalsIgnoreCase(distributorType)){
    	LOG.info("distributype = random.");
    	distributor = new RandomDistributor();
    }
    else if("roulette".equalsIgnoreCase(distributorType)){
    	LOG.info("distributype = roulette.");
    	distributor = new RouletteDistributor();
    }
    else if("workload".equalsIgnoreCase(distributorType)){
    	LOG.info("distributype = workload.");
    	distributor = new WorkloadEstimationDistributor();
    }
    else if("aworkload".equalsIgnoreCase(distributorType)){
    	LOG.info("distributype = accurate workload.");
    	distributor = new AccurateWEDistributor();
    	
    }
    distributor.initialization(configuration);
  }

  @Override
  public Collection<PartitionStats> call() {
    // Thread initialization (for locality)
    this.workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E, M>(
            context, configuration, serviceWorker);
    WorkerThreadAggregatorUsage aggregatorUsage =
        serviceWorker.getAggregatorHandler().newThreadAggregatorUsage();

    this.graphState = new GraphState<I, V, E, M>(graphState.getSuperstep(),
        graphState.getTotalNumVertices(), graphState.getTotalNumEdges(),
        context, graphState.getGraphTaskManager(), workerClientRequestProcessor,
        aggregatorUsage);

    List<PartitionStats> partitionStatsList = Lists.newArrayList();
    int lastPartitionId = -1;
    while (!partitionIdQueue.isEmpty()) {
      Integer partitionId = partitionIdQueue.poll();
      if (partitionId == null) {
        break;
      }
      lastPartitionId = partitionId;
      Partition<I, V, E, M> partition =
          serviceWorker.getPartitionStore().getPartition(partitionId);
      try {
        PartitionStats partitionStats = computePartition(partition);
        partitionStatsList.add(partitionStats);
        long partitionMsgs = workerClientRequestProcessor.resetMessageCount();
        partitionStats.addMessagesSentCount(partitionMsgs);
        messagesSentCounter.inc(partitionMsgs);
        timedLogger.info("call: Completed " +
            partitionStatsList.size() + " partitions, " +
            partitionIdQueue.size() + " remaining " +
            MemoryUtils.getRuntimeMemoryStats());
      } catch (IOException e) {
        throw new IllegalStateException("call: Caught unexpected IOException," +
            " failing.", e);
      } finally {
        serviceWorker.getPartitionStore().putPartition(partition);
      }
    }

    if (LOG.isInfoEnabled()) {
      float seconds = Times.getNanosSince(TIME, startNanos) /
          Time.NS_PER_SECOND_AS_FLOAT;
      LOG.info("call: Computation took " + seconds + " secs for "  +
          partitionStatsList.size() + " partitions on superstep " +
          graphState.getSuperstep() + ".  Flushing started");
    }
    try {
      workerClientRequestProcessor.flush();
      if(LOG.isInfoEnabled()){
 	     LOG.info("\n\tlastPartitionId="+lastPartitionId+
 	    		 "\n\tlocalRequestCost="+workerClientRequestProcessor.getLocalRequestCost()+
 	    		 "\n\tlocalRequest="+workerClientRequestProcessor.getLocalCounter()+  
	    		    "\n\tlocalByteCount="+workerClientRequestProcessor.getLocalByteCounter()+ 
 	    		   "\n\tremoteRequestCost="+workerClientRequestProcessor.getRemoteRequestCost()+
	        		"\n\tremoteRequest="+workerClientRequestProcessor.getRemoteCounter()+
	        		"\n\tremoteByteCount="+workerClientRequestProcessor.getRemoteByteCounter()+
	        		"\n\tmessageProcessed="+ totalMsgCount + 
	        		"\n\tmaxMsgProcessVertex="+maxMsgProcessByVertex+
	        		"\n\tmaxMsgProcessVertexId="+maxMsgProcessVertexId+
	        		"\n\tmaxMsgGenerationVertex="+maxMsgGenerationByVertex+
	        		"\n\tmaxMsgGenerationVertexId="+maxMsgGenerationVertexId+
	        		"\n\tsendMessage="+ totalSendMsgCount + 
	        		"\n\tinvalidByMatchedVertex="+ totalInvalidByMatchedVertex + 
	        		"\n\tinvalidByMatchedVertexIndex="+ totalInvalidByMatchedVertexIndex + 
	        		"\n\tinvalidByUnmatchedVertex="+ totalInvalidByUnmatchedVertex + 
	        		"\n\tmatchedSubgraphCount="+workerClientRequestProcessor.getMatchedSubgraphCount()+
	        		"\n\ttotalTimeCost="+totalTimeCost+
	        		"\n\ttotalCheckValidCost="+totalCheckValidCost+
	        		"\n\ttotalCheckOrientationCost="+totalCheckOrientationCost+
	        		"\n\ttotalCheckCandidateNeighborhoodCost="+totalCheckCandidateNeighborhoodCost+
	        		"\n\ttotalGetCandidateCost="+totalGetCandidateCost+
	        		"\n\ttotalInvalidByOrientation="+totalInvalidByOrientation+
	        		"\n\trealTimeCost="+realTimeCost+
	        		"\n\tdistributorCost="+distributorCost);
 	     if(distributor instanceof WorkloadEstimationDistributor || 
 	    		 distributor instanceof AccurateWEDistributor){
 	    	 LOG.info("Workload Message: "+ distributor.toString());
 	     }
   }
      aggregatorUsage.finishThreadComputation();
    } catch (IOException e) {
      throw new IllegalStateException("call: Flushing failed.", e);
    }
    
    if (LOG.isInfoEnabled()) {
        float seconds = Times.getNanosSince(TIME, startNanos) /
            Time.NS_PER_SECOND_AS_FLOAT;
        LOG.info("call: AllPartitionComputated took " + seconds + " secs for "  +
            partitionStatsList.size() + " partitions on superstep " +
            graphState.getSuperstep() + ".  Flushing started");
      }
    
    return partitionStatsList;
  }

  /**
   * Compute a single partition
   *
   * @param partition Partition to compute
   * @return Partition stats for this computed partition
   */
  private PartitionStats computePartition(Partition<I, V, E, M> partition)
    throws IOException {
    PartitionStats partitionStats =
        new PartitionStats(partition.getId(), 0, 0, 0, 0);
    // Make sure this is thread-safe across runs
    synchronized (partition) {
      // Prepare Partition context
      WorkerContext workerContext =
          graphState.getGraphTaskManager().getWorkerContext();
      PartitionContext partitionContext = partition.getPartitionContext();
      synchronized (workerContext) {
        partitionContext.preSuperstep(workerContext);
      }
      graphState.setPartitionContext(partition.getPartitionContext());

      for (Vertex<I, V, E, M> vertex : partition) {
        // Make sure every vertex has this thread's
        // graphState before computing
        vertex.setGraphState(graphState);
        Iterable<M> messages = messageStore.getVertexMessages(vertex.getId());
//        Iterables.size(messages);
        if (vertex.isHalted() && !Iterables.isEmpty(messages)) {
          vertex.wakeUp();
        }
        if (!vertex.isHalted()) {
          context.progress();
          TimerContext computeOneTimerContext = computeOneTimer.time();
          try {
        	if(LOG.isInfoEnabled()){
	        	vertex.resetMessgeCount();
	        	vertex.resetSendMessageCount();
	        	vertex.resetInvalidByMatchedVertex();
	        	vertex.resetInvalidByMatchedVertexIndex();
	        	vertex.resetInvalidByUnmatchedVertex();
	        	vertex.resetInvalidByOrientation();
	        	vertex.resetTimeCost();
	        	
	        	vertex.resetCheckCandidateNeighborhoodCost();
	        	vertex.resetCheckOrientationCost();
	        	vertex.resetCheckValidCost();
	        	vertex.resetGetCandidateCost();   
	        	
	        	vertex.resetDistributorTime();
	        	if(vertex.getBigEdges() == null || vertex.getSmallEdges() == null){
	        		System.err.print("vid= "+ vertex.getId()+", What is the hell!");
	        		System.exit(-1);
	        	}
        	}
        	
        	/* set the distributor here. */
        	vertex.setDistributor(distributor);
        	long start = System.currentTimeMillis();
        	
            vertex.compute(messages);
            
            realTimeCost += System.currentTimeMillis() - start;
            
            if(LOG.isInfoEnabled()){
	            totalMsgCount += vertex.getMessageCount();
	            if(vertex.getMessageCount() > maxMsgProcessByVertex){
	            	maxMsgProcessByVertex = vertex.getMessageCount();
	            	maxMsgProcessVertexId = Integer.valueOf(vertex.getId().toString());
	            }
	            totalSendMsgCount += vertex.getSendMessageCount();
	            
	            if(vertex.getSendMessageCount() > maxMsgGenerationByVertex){
	            	maxMsgGenerationByVertex = vertex.getSendMessageCount();
	            	maxMsgGenerationVertexId = Integer.valueOf(vertex.getId().toString()); 
	            }
	            
	            totalInvalidByMatchedVertex += vertex.getInvalidByMatchedVertex();
	            totalInvalidByMatchedVertexIndex += vertex.getInvalidByMatchedVertexIndex();
	            totalInvalidByUnmatchedVertex += vertex.getInvalidByUnmatchedVertex();
	            totalInvalidByOrientation += vertex.getInvalidByOrientation();
	            totalTimeCost += vertex.getTimeCost();
	            
	            totalCheckCandidateNeighborhoodCost += vertex.getCheckCandidateNeighborhoodCost();
	            totalCheckOrientationCost += vertex.getCheckOrientationCost();
	            totalCheckValidCost += vertex.getCheckValidCost();
	            totalGetCandidateCost += vertex.getGetCandidateCost();
	            distributorCost += vertex.getDistributorTime();
            }
          } finally {
            computeOneTimerContext.stop();
          }
          // Need to unwrap the mutated edges (possibly)
          vertex.unwrapMutableEdges();
          // Need to save the vertex changes (possibly)
          partition.saveVertex(vertex);
        }
        if (vertex.isHalted()) {
          partitionStats.incrFinishedVertexCount();
        }
        // Remove the messages now that the vertex has finished computation
        messageStore.clearVertexMessages(vertex.getId());

        // Add statistics for this vertex
        partitionStats.incrVertexCount();
        partitionStats.addEdgeCount(vertex.getNumEdges());
      }

      messageStore.clearPartition(partition.getId());

      synchronized (workerContext) {
        partitionContext.postSuperstep(workerContext);
      }
    }
    return partitionStats;
  }
}

