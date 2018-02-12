package org.apache.giraph.subgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.ComputeCallable;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.SimpleVertexWriter;
import org.apache.giraph.master.DefaultMasterCompute;
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
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class KTrussDecompositionSubgraph <I extends WritableComparable, V extends Writable,
E extends Writable, M extends Writable>
implements Callable<Collection<PartitionStats>> {
	/** Class logger */
	  private static final Logger LOG  = Logger.getLogger(KTrussDecompositionSubgraph.class);
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
	  /** VertexWriter for this ComputeCallable */
	  private SimpleVertexWriter<I, V, E> vertexWriter;
	  
	  /** Get the start time in nanos */
	  private final long startNanos = TIME.getNanoseconds();

	  // Per-Superstep Metrics
	  /** Messages sent */
	  private final Counter messagesSentCounter;
	  /** Timer for single compute() call */
	  private final Timer computeOneTimer;

	  private LocalKTrussAlgorithm lktruss = new LocalKTrussAlgorithm();
	  Queue<BasicEdge> queue = new LinkedList<BasicEdge>();


		private static final String CHANGE_AGG = "changeThreshold";
		private static final String GLOBAL_THRESHOLD = "threshold";
	  
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
	  public KTrussDecompositionSubgraph(
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
	    
	    lktruss.threshold = configuration.getInt("giraph.ktruss.threshold", 2) - 2;
		  
	  }
	  
	@Override
	public Collection<PartitionStats> call() throws Exception {
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

	/**
	   * Compute a single partition
	   * Here should be triditional algorithm
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

	      /* TODO: BEGIN: execute local algorithm on graph store here */
	      /* the size of the queue to indicate whether it is active of this subgraph */
	      try{
	    	  processSubgraph((BasicGraphStoreInterface)partition, partitionStats);
	      }catch(Exception e){
	    	  e.printStackTrace();
	      }

	      /* END! */

	      messageStore.clearPartition(partition.getId());

	      synchronized (workerContext) {
	        partitionContext.postSuperstep(workerContext);
	      }
	    }
	    return partitionStats;
	  }
	  
	  /**
	   * The main logical for subgraph processing
	   * @param graphStore
	   */
	  /* 
	   * send message to generate induced subgraph,
	   * here need to use the query mode. 
	   * query if two end points are remote
	   * <R1,R2>: R1,R2 is belong to the same partition
	   * <R1,R2>: R1,R2 is belong to the different partition
	   * */
	  /* 0. processing the incoming message, and update graph */
	  /* 1. repeatedly clean edge and vertex until it is stable. */
	  /* 2. add methods for partition stats, use the vertex count and 
	   * finished vertex count to track the state of the whole job*/
	  
	  /**
	   * Edge centric send message model.
	   * if edge has a common neighbor who is a remote one, 
	   * then whenever the state of edge is changed, 
	   * it should send a message to notice the remote one.
	   * 
	   *  How to process the duplicated one, and how to minimize the communication.
	   *  case one: the partition is fixed;
	   *  case two: the impacts of the partition.
	   *  First: we  should determine the whole logic here.
	   *  an edge in induced subgraph is deleted:
	   *  <L,L>: find a common neighbor who is remote one
	   *  <L,R>: find a common neighbor who is in different partition with the R in this edge.
	   *  <R,R>: never happen
	   */
	  private void processSubgraph(BasicGraphStoreInterface graphStore, PartitionStats partitionStats) 
			  throws Exception{
		  lktruss.threshold = ((IntWritable)getAggregatedValue(GLOBAL_THRESHOLD)).get() - 2;
		  
		  LOG.info("Local: Superstep="+graphState.getSuperstep()+": threshold="+lktruss.threshold);
		  
		  if(graphState.getSuperstep() > 2){
			  execution(graphStore, partitionStats);
		  }
		  else if(graphState.getSuperstep() == 2){
			  inducedSubgraphGeneration(graphStore, partitionStats);
		  }
		  else if(graphState.getSuperstep() == 1){
			  AnswerSuperstep(graphStore, partitionStats);
		  }
		  else if(graphState.getSuperstep() == 0){
			  QuerySuperstep(graphStore, partitionStats);
		  }
	  }
	  
	  /**
	   * Execute the local k truss in a superstep
	   * 
	   * NOTE:
	   * First, assure the same edge is in queue only once.
	   * Second, why the deleted edge appears again? The deleted edge should be deleted only once.
	   * @param graphStore
	   * @param partitionStats
	   */
	  private void execution(BasicGraphStoreInterface graphStore, PartitionStats partitionStats) throws Exception{
		  /* process the deleted edge<R,R> message, and initialize the queue */
		  HashMap<Long, Boolean> isInQueue = new HashMap<Long, Boolean>();
		  IntWritable id = new IntWritable();
		  queue.clear();
		  
		  /**
		   * Every time the threshold is updated,
		   * needs initialize again.
		   */
		  if(!((BooleanWritable)getAggregatedValue(CHANGE_AGG)).get()){
			  lktruss.initialize(graphStore, queue);
			  LOG.info("Superstep="+this.serviceWorker.getSuperstep()+" After initialization queue size="+queue.size()+"" +
			  		"\nVsize="+graphStore.getVertexCount()+" Esize="+graphStore.getEdgeCount());
		  }
		  else{
			  for(BasicVertex gv : graphStore.getLocalVertex()){
				  /* process message, generate induced subgraph */
				  id.set(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages((I)id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
			    		  
			    		  TripleWritable tmsg = (TripleWritable)msg;
	//		    		  LOG.info("msg ="+msg.toString()+" exist="+graphStore.edgeExist(tmsg.getThird(), tmsg.getSecond()));
			    		  long idx = BasicEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird());
			    		  if(graphStore.edgeExist(tmsg.getSecond(), tmsg.getThird()) 
			    				  && (isInQueue.get(idx) == null)){
			    			  isInQueue.put(idx, true);
			    			  queue.add(graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()));
			    		  }
			    	  }
			      }
			  }
		  }
		  
		  LOG.info("Queue Size="+queue.size());
		  
		  /*NOTE: process the subgraph, and send message */
		  if(!lktruss.iterativelyPrune(graphStore, queue)){
			  /* assume when the graph is empty, it will be finish automatically. As 
			   * finished vertex count = 0, total is 0 too. */
			 /* here it needs to identify the finishing point */
			 aggregate(CHANGE_AGG,  new BooleanWritable(false));
		  }
		  else{
			aggregate(CHANGE_AGG,  new BooleanWritable(true));
		  }
		  /* set partition stats here */
		  partitionStats.addVertexCount(graphStore.getVertexCount());
		  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  
	  }
	  
	  /**
	   * 1. Process the incoming message, generate induced subgraph with add edge, and initial the support value.
	   * 2. initialize the local k truss algorithm with insert the invalid edges into queue
	   * 3. execute the local k truss algorithm
	   * @param graphStore
	   * @param partitionStats
	   */
	  private void inducedSubgraphGeneration(BasicGraphStoreInterface graphStore, PartitionStats partitionStats) throws Exception{
		  int processedMsg = 0;
		  long start_time = System.currentTimeMillis(), mid_time, end_time, mid_time2;

		  IntWritable id = new IntWritable();
		  queue.clear();
		  for(BasicVertex gv : graphStore.getLocalVertex()){
			  id.set(gv.getId());
		      Iterable<M> messages = messageStore.getVertexMessages((I)id);
		      if (!Iterables.isEmpty(messages)) {
		    	  for(M msg : messages){
		    		  TripleWritable tmsg = (TripleWritable)msg;		    		  
		    		  /* add edge */
		    		  processedMsg++;
//		    		  LOG.info("msg: "+msg.toString());
		    		  graphStore.addEdge(new BasicEdge(tmsg.getSecond(), tmsg.getThird(), BasicEdge.BASICEDGE_EXTERNAL, false));
		    		  graphStore.addEdge(new BasicEdge(tmsg.getFirst(), tmsg.getThird(), BasicEdge.BASICEDGE_EXTERNAL, false));
		    		  graphStore.addEdge(new BasicEdge(tmsg.getSecond(), tmsg.getFirst(), BasicEdge.BASICEDGE_EXTERNAL, false));

		    		  /* no matter what type the edge is, increase the support value. */
		    		  graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()).incCount();
		    		  graphStore.getEdge(tmsg.getFirst(), tmsg.getThird()).incCount();
		    		  graphStore.getEdge(tmsg.getSecond(), tmsg.getFirst()).incCount();
		    	  }
		      }
		  }
		  
		  mid_time2 = System.currentTimeMillis();
		  /* remove insufficiently edges */
		  lktruss.initialize(graphStore, queue);
		  mid_time = System.currentTimeMillis();
		  LOG.info("After initialization queue size="+queue.size());
//		  graphStore.dump();
		  /* process the subgraph, and send message */
		  if(!lktruss.iterativelyPrune(graphStore, queue)){
			  /* no edge deleted and stop the algorithm */
			  aggregate(CHANGE_AGG,  new BooleanWritable(false));
		  }
		  else{
			aggregate(CHANGE_AGG,  new BooleanWritable(true));
		  }
		  end_time = System.currentTimeMillis();
		  LOG.info("message time="+(mid_time2-start_time)+
				  " initiali time="+(mid_time-mid_time2)+
				  " prune time="+(end_time-mid_time)+
				  " process message="+processedMsg);
		  
		  /* set partition stats here */
		  partitionStats.addVertexCount(graphStore.getVertexCount());
		  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  
	  }
	  
	  /**
	   * Answer the triads query.
	   * 
	   * traditional vertex program
	   * Comm. Complexity: less than 3 * triangle(eliminate duplication with message combine, see NOTE.)
	   * 
	   * NOTE: merge the outgoing message if they are to the same partition. Merge is not complete yet.
	   * Because we do not consider the different triangle situation.
	   * 
	   * @param graphStore
	   * @param partitionStats
	   */
	  private void AnswerSuperstep(BasicGraphStoreInterface graphStore, PartitionStats partitionStats) throws Exception{
		  IntWritable id = new IntWritable();
		  for(BasicVertex gv : graphStore.getLocalVertex()){
			  id.set(gv.getId());
		      Iterable<M> messages = messageStore.getVertexMessages((I)id);
		      if (!Iterables.isEmpty(messages)) {
		    	  for(M msg : messages){
		 
		    		  TripleWritable tmsg = (TripleWritable)msg;
		    		  if(gv.containNeighbor(tmsg.getThird())){
		    				  I tid = (I) new IntWritable(tmsg.getFirst());
		    				  M nmsg = (M) new TripleWritable(tmsg.getFirst(), tmsg.getSecond(), tmsg.getThird());
		    				  sendMessage(tid, nmsg);
		    				  
		    				  if(!isSamePartition(tmsg.getFirst(), tmsg.getSecond())){
		    					  tid = (I) new IntWritable(tmsg.getSecond());
		    					  nmsg = (M) new TripleWritable(tmsg.getSecond(), tmsg.getFirst(), tmsg.getThird());
		    					  sendMessage(tid, nmsg);
		    				  }
		    				  
		    				  if(!isSamePartition(tmsg.getFirst(), tmsg.getThird()) 
		    						  && !isSamePartition(tmsg.getSecond(), tmsg.getThird())){
		    					  tid = (I) new IntWritable(tmsg.getThird());
		    					  nmsg = (M) new TripleWritable(tmsg.getThird(), tmsg.getFirst(), tmsg.getSecond());
		    					  sendMessage(tid, nmsg);
		    				  }
		    			  }
		    	  }
		      }
		  }
			aggregate(CHANGE_AGG,  new BooleanWritable(true));
		  /* collect data */
		  partitionStats.addVertexCount(graphStore.getVertexCount());
		  partitionStats.addEdgeCount(graphStore.getEdgeCount());
	  }
	  
	  /**
	   * same as triangle count. 
	   * The input graph should be ordered
	   * Enumerate triads from the lowest degree vertex
	   * Comm. Complexity for each vertex: ds^2
	   * @param graphStore
	   * @param partitionStats
	   */
	  private void QuerySuperstep(BasicGraphStoreInterface graphStore, PartitionStats partitionStats)  throws Exception{
		  for(BasicVertex gv : graphStore.getLocalVertex()){
			  for(BasicEdge nb1 : gv.getNeighbors()){
				  if(gv.getId() >= nb1.getTargetId()) continue;
				  for(BasicEdge nb2 : gv.getNeighbors()){
					  if(gv.getId() < nb2.getTargetId()
							  && nb1.getTargetId() < nb2.getTargetId()){
						  /* send message */
						  I id = (I)new IntWritable(nb1.getTargetId());
						  M msg = (M)new TripleWritable(gv.getId(), nb1.getTargetId(), nb2.getTargetId());
//						  LOG.info("superstep: 0 msg: "+msg.toString());
						  sendMessage(id, msg);
					  }
				  }
			  }
		  }
			aggregate(CHANGE_AGG,  new BooleanWritable(true));
		  /* collect data */
		  partitionStats.addVertexCount(graphStore.getVertexCount());
		  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  
//		  graphStore.dump();
	  }
	  
	  private void sendMessage(I id, M message) {
	    if (graphState.getWorkerClientRequestProcessor().
	          sendMessageRequest(id, message)) {
	      graphState.getGraphTaskManager().notifySentMessages();
	    }
	}
	  
	  public <A extends Writable> A getAggregatedValue(String name) {
	    return graphState.getWorkerAggregatorUsage().<A>getAggregatedValue(name);
	  }
	  
	  public <A extends Writable> void aggregate(String name, A value) {
		    graphState.getWorkerAggregatorUsage().aggregate(name, value);
	  }
	  
	  public boolean isSamePartition(int vid1, int vid2){
		  return graphState.getGraphTaskManager().isSamePartition(vid1, vid2);
	  }
	  
	  /** 
	   * Master compute which uses aggregators. 
	   * */
		public static class AggregatorsMasterCompute extends
		DefaultMasterCompute {
			private int currentThreshold;
			@Override
			public void compute() {
				/**
				 * When all works say change[false], then change it.
				 */
				if(!((BooleanWritable)getAggregatedValue(CHANGE_AGG)).get()){
					currentThreshold++;
					System.out.println("Superstep="+getSuperstep()+" aggreagatedVaule: ("+CHANGE_AGG+
							" " + ((BooleanWritable)getAggregatedValue(CHANGE_AGG))+")"
							+" threshold="+currentThreshold);
				}
		        setAggregatedValue(GLOBAL_THRESHOLD, new IntWritable(currentThreshold));
			}

			@Override
			public void initialize() throws InstantiationException,
	        IllegalAccessException {
				this.currentThreshold = 2;
		        registerAggregator(GLOBAL_THRESHOLD, IntSumAggregator.class);
				registerAggregator(CHANGE_AGG, BooleanOrAggregator.class);
			}
	  }
	  
/**
 * This class manage the local ktruss algorithm
 * @author simon0227
 *
 */
	  public class LocalKTrussAlgorithm {
			
			public int threshold;
//			private int deleteEdge = 0;
			
//			public int getDeletedEdge(){
//				return deleteEdge;
//			}
			
			/**
			 * Need to remove isolated vertex here!
			 * @param graphStore
			 * @param queue
			 */
			public boolean iterativelyPrune(BasicGraphStoreInterface graphStore, Queue<BasicEdge> queue){
				if(queue.isEmpty()){
					return false;
				}
//				LOG.info("Beging Iteratively Pruning......");
				while(!queue.isEmpty()){
					BasicEdge deleteEdge = queue.poll();
					graphStore.deleteEdge(deleteEdge.getSourceId(), deleteEdge.getTargetId());
//					LOG.info("Delete Edge: "+deleteEdge.toString() + " threshold="+this.threshold);
//					graphStore.dump();
					oneIteration(graphStore, deleteEdge, queue);
				}
				return true;
			}
			
			/**
			 * Consider the deleted edge's influence,
			 * when it is eliminated from the queue.
			 * @param graphStore
			 * @param queue
			 */
			public void initialize(BasicGraphStoreInterface graphStore, Queue<BasicEdge> queue){
				long time = 0;
				for(BasicEdge edge : graphStore.getEdges()){
					/* do not process external edge */
					if(edge.isExternal()){
//						externalEdge++;
						continue;
					}
					
					if(edge.getCount() < threshold){
						queue.add(edge);
					}
				}
			}
			
			/**
			 * 1. update neighbored edges count, if they are internal and cross
			 * 2. send message to the vertex, if the deleted edge has a common neighbor who is remote. 
			 * @param graphStore
			 * @param deleteEdge
			 * @param queue
			 */
			public void oneIteration(BasicGraphStoreInterface graphStore, BasicEdge deleteEdge, Queue<BasicEdge> queue){
				int sourceId = deleteEdge.getSourceId();
				int targetId = deleteEdge.getTargetId();
				
				int degree1 = graphStore.getDegree(sourceId);
				int degree2 = graphStore.getDegree(targetId);
				if(degree1 == 0 || degree2 == 0)
					return ;
				
				/* choose the vertex of smaller degree as the pivot */
				int pivot = degree1 > degree2 ? targetId : sourceId;
				int first = (pivot == sourceId) ? targetId : sourceId;
				
				for(BasicEdge nb : graphStore.getVertex(pivot).getNeighbors()){
					/**
					 * We should assure that the triangle is still exist.
					 */
					if(graphStore.edgeExist(first, nb.getTargetId())){
						/* 1. update neighbor */
						if(!graphStore.getEdge(first, nb.getTargetId()).isExternal()){
							/**
							 * Because we make sure the edge support count decrease one by one.
							 * Here should be the main copy?????????
							 */
							if(graphStore.getMainEdge(first, nb.getTargetId()).decAndGetCount() + 1 == threshold){
								queue.add(graphStore.getMainEdge(first, nb.getTargetId()));
							}
						}
						if(!graphStore.getEdge(pivot, nb.getTargetId()).isExternal()){
							if(graphStore.getMainEdge(pivot, nb.getTargetId()).decAndGetCount() + 1 ==  threshold){
								queue.add(graphStore.getMainEdge(pivot, nb.getTargetId()));
							}
						}
						
						/* 2. track notify list */
						if(!deleteEdge.isExternal() 
								&& !graphStore.isLocal(nb.getTargetId())
								&& !isSamePartition(pivot, nb.getTargetId())
								&& !isSamePartition(first, nb.getTargetId())){
							I id = (I) new IntWritable(nb.getTargetId());
							M msg = (M)new TripleWritable(sourceId, sourceId, targetId);
//							LOG.info("Send remove message: "+msg+" to"+vid);
							sendMessage(id, msg);
						}
					}
				}
			}
		}
}
