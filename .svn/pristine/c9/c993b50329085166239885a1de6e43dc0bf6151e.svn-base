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
package org.apache.giraph.subgraph.graphextraction;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.io.SimpleVertexWriter;
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

import com.google.common.collect.Lists;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

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
@SuppressWarnings("rawtypes")
public abstract class GraphExtractionCallable<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable>
    implements Callable<Collection<PartitionStats>> {
  /** Class logger */
  protected static final Logger LOG  = Logger.getLogger(GraphExtractionCallable.class);
  /** Class time object */
  protected static final Time TIME = SystemTime.get();
  /** Context */
  protected final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state (note that it is recreated in call() for locality) */
  protected GraphState<I, V, E, M> graphState;
  /** Thread-safe queue of all partition ids */
  protected final BlockingQueue<Integer> partitionIdQueue;
  /** Message store */
  protected final MessageStoreByPartition<I, M> messageStore;
  /** Configuration */
  protected final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** Worker (for NettyWorkerClientRequestProcessor) */
  protected final CentralizedServiceWorker<I, V, E, M> serviceWorker;
  /** Dump some progress every 30 seconds */
  protected final TimedLogger timedLogger = new TimedLogger(30 * 1000, LOG);
  /** Sends the messages (unique per Callable) */
  protected WorkerClientRequestProcessor<I, V, E, M>
  workerClientRequestProcessor;
  /** VertexWriter for this ComputeCallable */
  protected SimpleVertexWriter<I, V, E> vertexWriter;
  /** Get the start time in nanos */
  protected final long startNanos = TIME.getNanoseconds();

  // Per-Superstep Metrics
  /** Messages sent */
  protected final Counter messagesSentCounter;
  /** Timer for single compute() call */
  protected final Timer computeOneTimer;

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
  public GraphExtractionCallable(
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

    vertexWriter = serviceWorker.getSuperstepOutput().getVertexWriter();

    List<PartitionStats> partitionStatsList = Lists.newArrayList();
    while (!partitionIdQueue.isEmpty()) {
      Integer partitionId = partitionIdQueue.poll();
      if (partitionId == null) {
        break;
      }

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
      } catch (InterruptedException e) {
        throw new IllegalStateException("call: Caught unexpected " +
            "InterruptedException, failing.", e);
      } finally {
        serviceWorker.getPartitionStore().putPartition(partition);
      }
    }

    // Return VertexWriter after the usage
    serviceWorker.getSuperstepOutput().returnVertexWriter(vertexWriter);

    if (LOG.isInfoEnabled()) {
      float seconds = Times.getNanosSince(TIME, startNanos) /
          Time.NS_PER_SECOND_AS_FLOAT;
      LOG.info("call: Computation took " + seconds + " secs for "  +
          partitionStatsList.size() + " partitions on superstep " +
          graphState.getSuperstep() + ".  Flushing started");
    }
    try {
      workerClientRequestProcessor.flush();
      
      aggregatorUsage.finishThreadComputation();
    } catch (IOException e) {
      throw new IllegalStateException("call: Flushing failed.", e);
    }
    return partitionStatsList;
  }
  
  public <A extends Writable> void aggregate(String name, A value) {
    graphState.getWorkerAggregatorUsage().aggregate(name, value);
  }

  /**
   * Compute a single partition
   * 1. get executing query
   * 2. retrieve vertices and their negihbors
   * 3. construct new 2-length paths.
   *
   * @param partition Partition to compute
   * @return Partition stats for this computed partition
   */
  private PartitionStats computePartition(Partition<I, V, E, M> partition)
    throws IOException, InterruptedException {
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

      // the real execution logic here!!
      computeSuperstep(partition, partitionStats);

      messageStore.clearPartition(partition.getId());

      synchronized (workerContext) {
        partitionContext.postSuperstep(workerContext);
      }
    }
    return partitionStats;
  }
  
  /**
   * computation logic in a superstep.
   * 
   * @param partition
   * @param partitionStats
   * @throws IOException
   * @throws InterruptedException
   */
  public abstract void computeSuperstep(Partition<I, V, E, M> partition,
		PartitionStats partitionStats) throws IOException, InterruptedException;
  
}

