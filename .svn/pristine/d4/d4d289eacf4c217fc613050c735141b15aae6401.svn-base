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
import org.apache.giraph.aggregators.LongSumAggregator;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class KTrussSubgraphImpr <I extends WritableComparable, V extends Writable,
E extends Writable, M extends Writable>
implements Callable<Collection<PartitionStats>> {
	/** Class logger */
	  private static final Logger LOG  = Logger.getLogger(KTrussSubgraphImpr.class);
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
	  
//	  private static final String AGG_TOTALEDGE = "totalEdge";
//	  private static final String AGG_EDGE = "validEdge";
//	  
//	  private static final String AGG_MSG_TCSUBGRAPH = "tcsubgraphmsg";
//	  private static final String AGG_MSG_DETECTION = "detectionmsg";
//	  private static final String AGG_MSG_INDEPENDENT = "independentmsg";
	  
	  private long msgTcSubgraphCounter = 0;
	  private long msgDetectionCounter = 0;
	  private long independedExternalRemoval = 0;
	  
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
	  public KTrussSubgraphImpr(
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
		  if(graphState.getSuperstep() > 2){
			  this.msgDetectionCounter = 0;
			  this.independedExternalRemoval = 0;
			  execution(graphStore, partitionStats);
//			  aggregate(AGG_MSG_DETECTION, new LongWritable(this.msgDetectionCounter));
//			  aggregate(AGG_MSG_INDEPENDENT, new LongWritable(this.independedExternalRemoval));
		  }
		  else if(graphState.getSuperstep() == 2){
			  this.msgDetectionCounter = 0;
			  inducedSubgraphGeneration(graphStore, partitionStats);
//			  aggregate(AGG_MSG_DETECTION, new LongWritable(this.msgDetectionCounter));
		  }
		  else if(graphState.getSuperstep() == 1){
			  this.msgTcSubgraphCounter = 0;
			  AnswerSuperstep(graphStore, partitionStats);
//			  aggregate(AGG_MSG_TCSUBGRAPH, new LongWritable(this.msgTcSubgraphCounter));
		  }
		  else if(graphState.getSuperstep() == 0){
			  this.msgTcSubgraphCounter = 0;
			  QuerySuperstep(graphStore, partitionStats);
//			  aggregate(AGG_MSG_TCSUBGRAPH, new LongWritable(this.msgTcSubgraphCounter));
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
		    			  this.independedExternalRemoval++;
//		    			  if(!graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()).isExternal()){
//		    				  LOG.info("The second FUCK thing!!!!!!!!!");
//		    			  }
		    			  queue.add(graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()));
		    		  }
		    	  }
		      }
		  }
//		  LOG.info("After removing edges queue size ="+queue.size());
//		  LOG.info("Initial Edge Size="+graphStore.getEdges().size()+" "+graphStore.getVertexCount());
		  lktruss.clearMessageCount();
		  /*NOTE: process the subgraph, and send message */
		  if(!lktruss.iterativelyPrune(graphStore, queue)){
			 partitionStats.addFinishedVertexCount(graphStore.getVertexCount());
		  }

//		  LOG.info("After Pruning Edge Size="+graphStore.getEdges().size()+" "+graphStore.getVertexCount()+" msg="+lktruss.getMessageCount());
		  this.msgDetectionCounter += lktruss.getMessageCount();
//		  graphStore.dump();


//		  LOG.info("Delete Edge after prune = "+lktruss.getDeletedEdge()+" vsize="+graphStore.getVertexCount());
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
		  BasicEdge beStub = new BasicEdge();
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
		    		  beStub.initialize(tmsg.getSecond(), tmsg.getThird(), BasicEdge.BASICEDGE_EXTERNAL, false);
//		    		  graphStore.addEdge(new BasicEdge(tmsg.getSecond(), tmsg.getThird(), BasicEdge.BASICEDGE_EXTERNAL, false));
		    		  graphStore.addEdge(beStub);
		    		  
		    		  beStub.initialize(tmsg.getFirst(), tmsg.getThird(), BasicEdge.BASICEDGE_EXTERNAL, false);
//		    		  graphStore.addEdge(new BasicEdge(tmsg.getFirst(), tmsg.getThird(), BasicEdge.BASICEDGE_EXTERNAL, false));
		    		  graphStore.addEdge(beStub);
		    		  
		    		  beStub.initialize(tmsg.getSecond(), tmsg.getFirst(), BasicEdge.BASICEDGE_EXTERNAL, false);
//		    		  graphStore.addEdge(new BasicEdge(tmsg.getSecond(), tmsg.getFirst(), BasicEdge.BASICEDGE_EXTERNAL, false));
		    		  graphStore.addEdge(beStub);

		    		  /* no matter what type the edge is, increase the support value. */
		    		  graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()).incCount();
		    		  graphStore.getEdge(tmsg.getFirst(), tmsg.getThird()).incCount();
		    		  graphStore.getEdge(tmsg.getSecond(), tmsg.getFirst()).incCount();
		    	  }
		      }
		  }

//		  aggregate(AGG_TOTALEDGE,  new LongWritable(graphStore.getTotalEdgeCount()));
//		  aggregate(AGG_EDGE,  new LongWritable(graphStore.getEdgeCount()));
		  
		  mid_time2 = System.currentTimeMillis();
//		  LOG.info("After adding edges Superstep="+superstep);
//		  graphStore.dump();
//		  LOG.info("Initial Edge Size="+graphStore.getEdges().size()+" "+graphStore.getVertexCount());
		  /* remove insufficiently edges */
		  lktruss.initialize(graphStore, queue);
		  lktruss.clearMessageCount();
		  mid_time = System.currentTimeMillis();
//		  LOG.info("Delete Edge after initialize = "+lktruss.getDeletedEdge()+" vertexsize="+graphStore.getVertexCount());
//		  LOG.info("After initialization queue size="+queue.size());
//		  graphStore.dump();
		  /* process the subgraph, and send message */
		  if(!lktruss.iterativelyPrune(graphStore, queue)){
			  /* no edge deleted and stop the algorithm */
			  partitionStats.addFinishedVertexCount(graphStore.getVertexCount());
		  }
		  this.msgDetectionCounter += lktruss.getMessageCount();
		  
		  end_time = System.currentTimeMillis();
		  System.out.println("Message time="+(mid_time2-start_time)+
				  " Initiali time="+(mid_time-mid_time2)+
				  " Prune time="+(end_time-mid_time)+
				  " Process message="+processedMsg);
		  
//		  LOG.info("After pruning="+superstep);
//		  graphStore.dump();
//		  LOG.info("Delete Edge after prune = "+lktruss.getDeletedEdge()+" vsize="+graphStore.getVertexCount());
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
//		    		  LOG.info("msg: "+msg.toString()+" cur vid="+gv.getId());
		 
		    		  TripleWritable tmsg = (TripleWritable)msg;
//		    		  for(BasicEdge nb : gv.getNeighbors()){
//		    			  if(nb.getTargetId() == tmsg.getThird()){
		    		  /*NOTE: this optimization is really important. */
		    		  if(gv.containNeighbor(tmsg.getThird())){
		    				  I tid = (I) new IntWritable(tmsg.getFirst());
		    				  M nmsg = (M) new TripleWritable(tmsg.getFirst(), tmsg.getSecond(), tmsg.getThird());
		    				  sendMessage(tid, nmsg);
		    				  if(!isSamePartition(tmsg.getFirst(), gv.getId()))
		    					  this.msgTcSubgraphCounter++;
		    				  
		    				  if(!isSamePartition(tmsg.getFirst(), tmsg.getSecond())){
		    					  tid = (I) new IntWritable(tmsg.getSecond());
		    					  nmsg = (M) new TripleWritable(tmsg.getSecond(), tmsg.getFirst(), tmsg.getThird());
		    					  sendMessage(tid, nmsg);
		    					  if(!isSamePartition(tmsg.getSecond(), gv.getId()))
		    						  this.msgTcSubgraphCounter++;
		    				  }
		    				  
		    				  if(!isSamePartition(tmsg.getFirst(), tmsg.getThird()) 
		    						  && !isSamePartition(tmsg.getSecond(), tmsg.getThird())){
		    					  tid = (I) new IntWritable(tmsg.getThird());
		    					  nmsg = (M) new TripleWritable(tmsg.getThird(), tmsg.getFirst(), tmsg.getSecond());
		    					  sendMessage(tid, nmsg);
		    					  if(!isSamePartition(tmsg.getThird(), gv.getId()))
		    						  this.msgTcSubgraphCounter++;
		    				  }
		    			  }
		    		  }
//		    	  }
		      }
		  }
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
			  
			  ArrayList<Integer> smallList = new ArrayList<Integer>();

			  for(BasicEdge nb1 : gv.getNeighbors()){
				  if(gv.getId() < nb1.getTargetId()){
					  smallList.add(nb1.getTargetId());
				  }
			  }
			  int size = smallList.size();
			  for(int i = 0; i < size; i++){
				  int vidOne = smallList.get(i);
				  for(int j = i+1; j < size; j++){
					  int vidTwo = smallList.get(j);
					  if(vidOne < vidTwo){
						  /* send message */
						  I id = (I)new IntWritable(vidOne);
						  M msg = (M)new TripleWritable(gv.getId(), vidOne, vidTwo);
//						  LOG.info("superstep: 0 msg: "+msg.toString());
						  sendMessage(id, msg);
						  
						  if(!isSamePartition(vidOne, gv.getId()))
							  this.msgTcSubgraphCounter++;
					  }
					  else{
						  /* send message */
						  I id = (I)new IntWritable(vidTwo);
						  M msg = (M)new TripleWritable(gv.getId(), vidTwo, vidOne);
//						  LOG.info("superstep: 0 msg: "+msg.toString());
						  sendMessage(id, msg);
						  
						  if(!isSamePartition(vidTwo, gv.getId()))
							  this.msgTcSubgraphCounter++;
					  }
				  }
			  }
			  
//			  for(BasicEdge nb1 : gv.getNeighbors()){
//				  if(gv.getId() >= nb1.getTargetId()) continue;
//				  for(BasicEdge nb2 : gv.getNeighbors()){
//					  if(gv.getId() < nb2.getTargetId()
//							  && nb1.getTargetId() < nb2.getTargetId()){
//						  /* send message */
//						  I id = (I)new IntWritable(nb1.getTargetId());
//						  M msg = (M)new TripleWritable(gv.getId(), nb1.getTargetId(), nb2.getTargetId());
////						  LOG.info("superstep: 0 msg: "+msg.toString());
//						  sendMessage(id, msg);
//						  
//						  if(!isSamePartition(nb1.getTargetId(), gv.getId()))
//							  this.msgTcSubgraphCounter++;
//					  }
//				  }
//			  }
		  }
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
//	    sendMsgCounter++;
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
	   * This aggregator is used to aggregate the initial edges. 
	   * */
		public static class AggregatorsMasterCompute extends
		DefaultMasterCompute {
			
		
			
			@Override
			public void compute() {
//				if(getSuperstep() == 3){ /* after tc-subgraph constructed */
//		    		long totalEdge = ((LongWritable)getAggregatedValue(AGG_TOTALEDGE)).get();
//		    		long edge = ((LongWritable)getAggregatedValue(AGG_EDGE)).get();
//		    		System.out.println("TotalEdges: "+totalEdge+
//		    				" UniqueEdges: "+edge+
//		    				" Factor: "+ (totalEdge*1.0/edge));
////					LOG.info("Superstep="+getSuperstep()+" aggreagatedVaule: ("+AGG_TOTALEDGE+
////						"= " + ((LongWritable)getAggregatedValue(AGG_TOTALEDGE))+
////						"," +AGG_EDGE+"= "+((LongWritable)getAggregatedValue(AGG_EDGE))+")");
//				}
//				
//				long detection = ((LongWritable)getAggregatedValue(AGG_MSG_DETECTION)).get();
//				long tcsubgraph = ((LongWritable)getAggregatedValue(AGG_MSG_TCSUBGRAPH)).get();
//				long independent = ((LongWritable)getAggregatedValue(AGG_MSG_INDEPENDENT)).get();
//				
//				System.out.println("step= "+getSuperstep()+": tcsubgraph="+tcsubgraph+" detection="+detection+" independent="+independent);
			}

			@Override
			public void initialize() throws InstantiationException,
	        IllegalAccessException {
//		        registerAggregator(AGG_TOTALEDGE, LongSumAggregator.class);
//		        registerAggregator(AGG_EDGE, LongSumAggregator.class);
//		        
//		        this.registerPersistentAggregator(AGG_MSG_TCSUBGRAPH, LongSumAggregator.class);
//		        this.registerPersistentAggregator(AGG_MSG_DETECTION, LongSumAggregator.class);
//		        this.registerPersistentAggregator(AGG_MSG_INDEPENDENT, LongSumAggregator.class);
			}
	  }
	  
/**
 * This class manage the local ktruss algorithm
 * @author simon0227
 *
 */
	  public class LocalKTrussAlgorithm {
			
			public int threshold;
			private int sendMessages = 0;
			
			public int getMessageCount(){
				return sendMessages;
			}
			
			public void clearMessageCount(){
				sendMessages = 0;
			}
			
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
//					LOG.info("Delete Edge: "+deleteEdge.toString());
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
//				int externalEdge = 0;
//				System.out.println("Edge Size="+graphStore.getEdges().size()+" threshold="+threshold);
				long time = 0;
				for(BasicEdge edge : graphStore.getEdges()){
//					LOG.info(" initial edge: "+edge.toString());
					/* do not process external edge */
					if(edge.isExternal()){
//						externalEdge++;
						continue;
					}
					
//					int degree1 = graphStore.getDegree(edge.getSmallEndPoint());
//					int degree2 = graphStore.getDegree(edge.getBigEndPoint());
//					
//					/* choose the vertex of smaller degree as the pivot */
//					int pivot = degree1 > degree2 ? edge.getBigEndPoint() : edge.getSmallEndPoint();
//					int first = (pivot == edge.getSmallEndPoint()) ? edge.getBigEndPoint() : edge.getSmallEndPoint();
//					
//					int count = 0;
//					for(int vid : graphStore.getVertex(pivot).getNeighbors()){
//						long start_time = System.currentTimeMillis();
//						if(graphStore.edgeExist(first, vid)){
//							count++;
//						}
//						time += System.currentTimeMillis() - start_time;
//					}
//					edge.setCount(count);
					if(edge.getCount() < threshold){
						queue.add(edge);
					}
//					if(LOG.isInfoEnabled()){
//						LOG.info("After Initialization: BasicEdge "+ edge.toString());
//					}
				}
//				LOG.info("After initialization........");
//				graphStore.dump();
//				System.out.println("Time cost in edge retreival="+time);
//				LOG.info("External Edge="+externalEdge);
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
							 * NOTE: the +1 make sure that only in queue once.
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
//							if(deleteEdge.isExternal()){
//								LOG.info("source="+graphStore.isLocal(sourceId)+" target="+graphStore.isLocal(targetId)+
//										" third="+graphStore.isLocal(nb.getTargetId())+"What is FUCK thing!!!!!!!!!!!!!!");
//							}
							this.sendMessages ++;
							sendMessage(id, msg);
						}
					}
				}
			}
		}
}
