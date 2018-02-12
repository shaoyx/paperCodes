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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.ComputeCallable;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
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

public class KTrussSubgraph <I extends WritableComparable, V extends Writable,
E extends Writable, M extends Writable>
implements Callable<Collection<PartitionStats>> {
	/** Class logger */
	  private static final Logger LOG  = Logger.getLogger(KTrussSubgraph.class);
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
	  public KTrussSubgraph(
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
	    	  processSubgraph((GraphStoreInterface)partition, partitionStats);
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
	  private void processSubgraph(GraphStoreInterface graphStore, PartitionStats partitionStats) 
			  throws Exception{
		  int superstep = (int)graphState.getSuperstep();
		  
		  Queue<RawEdge> queue = new LinkedList<RawEdge>();
		  LocalKTrussAlgorithm lktruss = new LocalKTrussAlgorithm();
		  lktruss.threshold = configuration.getInt("giraph.ktruss.threshold", 2) - 2;
		  if(superstep > 2){
			  /* process the deleted edge<R,R> message, and initialize the queue */
			  HashMap<Long, Boolean> isInQueue = new HashMap<Long, Boolean>();
			  for(RawVertex gv : graphStore.getVertexs()){
				  /* process message, generate induced subgraph */
				  I id = (I)new IntWritable(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages(id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
			    		  TripleWritable tmsg = (TripleWritable)msg;
//			    		  LOG.info("delete msg: "+msg.toString());
			    		  /* update queue before delete the edge and update the related edge's count*/
//			    		  graphStore.deleteEdge(tmsg.getSecond(), tmsg.getThird());
//			    		  lktruss.oneIteration(graphStore, graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()), queue);
			    		  /* equivalent to the initialization phase*/
//			    		  if(!graphStore.getEdge(RawEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird())).isDelete())
			    		  if(!graphStore.getEdge(RawEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird())).isExternal() ||
			    				  graphStore.getEdge(RawEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird())).isDelete()){
			    			  LOG.warn("External Edge: "+ graphStore.getEdge(RawEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird())).isExternal()+
			    					  " Delete: "+graphStore.getEdge(RawEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird())).isDelete());
			    		  }
			    		  /**
			    		   * First, assure the same edge is in queue only once.
			    		   * Second, why the deleted edge appears again?
			    		   */
			    		  long idx = RawEdge.constructEdgeId(tmsg.getSecond(), tmsg.getThird());
			    		  if(!graphStore.getEdge(idx).isDelete() 
			    				  && (isInQueue.get(idx) == null)){
			    			  isInQueue.put(idx, true);
			    			  queue.add(graphStore.getEdge(idx));
			    		  }
			    	  }
			      }
			  }
//			  LOG.info("After removing edges Superstep="+superstep);
//			  graphStore.dump();
			  LOG.info("Initial Edge Size="+graphStore.getEdges().size()+" "+graphStore.getVertexCount());
			  
			  /*NOTE: process the subgraph, and send message */
			  if(!lktruss.iterativelyPrune(graphStore, queue)){
				 partitionStats.addFinishedVertexCount(graphStore.getVertexCount());
			  }


			  LOG.info("Delete Edge after prune = "+lktruss.getDeletedEdge()+" vsize="+graphStore.getVertexCount());
			  /* set partition stats here */
			  partitionStats.addVertexCount(graphStore.getVertexCount());
			  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  }
		  else if(superstep == 2){
			  /* generate the induced subgraph, initialize the queue */
			  int addEdge=0;
			  for(RawVertex gv : graphStore.getVertexs()){
				  /* process message, generate induced subgraph */
				  I id = (I)new IntWritable(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages(id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
			    		  TripleWritable tmsg = (TripleWritable)msg;		    		  
			    		  /* add edge */
			    		  addEdge++;
			    		  graphStore.addEdge(new RawEdge(tmsg.getSecond(), tmsg.getThird(), RawEdge.RAWEDGE_EXTERNAL));
			    	  }
			      }
			  }

//			  LOG.info("After adding edges Superstep="+superstep+" AddEdge="+addEdge);
//			  graphStore.dump();
			  LOG.info("Initial Edge Size="+graphStore.getEdges().size()+" "+graphStore.getVertexCount());
			  /* remove insufficiently edges */
			  lktruss.initialize(graphStore, queue);
			  LOG.info("Delete Edge after initialize = "+lktruss.getDeletedEdge()+" vertexsize="+graphStore.getVertexCount());
			  
			  /* process the subgraph, and send message */
			  if(!lktruss.iterativelyPrune(graphStore, queue)){
				  /* no edge deleted and stop the algorithm */
				  partitionStats.addFinishedVertexCount(graphStore.getVertexCount());
			  }

			  LOG.info("Delete Edge after prune = "+lktruss.getDeletedEdge()+" vsize="+graphStore.getVertexCount());
			  /* set partition stats here */
			  partitionStats.addVertexCount(graphStore.getVertexCount());
			  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  }
		  else if(superstep == 1){
			  /* answer the query from previous superstep */
			  for(RawVertex gv : graphStore.getVertexs()){
				  /* process message, send reply message  */
				  I id = (I)new IntWritable(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages(id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
//			    		  LOG.info("msg: "+msg.toString());
			    		  TripleWritable tmsg = (TripleWritable)msg;
			    		  for(int vid : gv.getNeighbors()){
			    			  if(vid == tmsg.getThird()){
			    				  I tid = (I) new IntWritable(tmsg.getFirst());
			    				  M nmsg = (M) new TripleWritable(gv.getId(), tmsg.getSecond(), tmsg.getThird());
			    				  sendMessage(tid, nmsg);
			    			  }
			    		  }
			    	  }
			      }

				  /* collect data */
				  if(gv.isLocal()){
					  partitionStats.incrVertexCount();
					  partitionStats.addEdgeCount(gv.getNeighbors().size());
				  }
			  }
		  }
		  else if(superstep == 0){
			  /* query the existence of edge<R,R> and complete the triangle */
//			  LOG.info("After input Superstep="+superstep);
//			  graphStore.dump();
			  for(RawVertex gv : graphStore.getVertexs()){

				  if(!gv.isLocal()) continue;

				  for(int vid : gv.getNeighbors()){
					  for(int pvid : gv.getNeighbors()){
						  if(vid < pvid 
								  && !graphStore.getVertex(vid).isLocal()
								  && !graphStore.getVertex(pvid).isLocal()){
							  /* send message */
							  I id = (I)new IntWritable(vid);
							  M msg = (M)new TripleWritable(gv.getId(), vid, pvid);
//							  LOG.info("superstep: 0 msg: "+msg.toString());
							  sendMessage(id, msg);
						  }
					  }
				  }
				  
				  /* collect data */
				  partitionStats.incrVertexCount();
				  partitionStats.addEdgeCount(gv.getNeighbors().size());
				  
			  }
		  }

//		  LOG.info("Out Superstep="+superstep);
//		  graphStore.dump();
	  }

	  private void processSubgraphWithTriangleCount(GraphStoreInterface graphStore, PartitionStats partitionStats) 
			  throws Exception{
		  int superstep = (int)graphState.getSuperstep();
		  
		  Queue<RawEdge> queue = new LinkedList<RawEdge>();
		  LocalKTrussAlgorithm lktruss = new LocalKTrussAlgorithm();
		  lktruss.threshold = configuration.getInt("giraph.ktruss.threshold", 2) - 2;
		  if(superstep > 2){
			  /* process the deleted edge<R,R> message, and initialize the queue */
			  for(RawVertex gv : graphStore.getVertexs()){
				  /* process message, generate induced subgraph */
				  I id = (I)new IntWritable(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages(id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
			    		  TripleWritable tmsg = (TripleWritable)msg;
//			    		  LOG.info("delete msg: "+msg.toString());
			    		  /* update queue before delete the edge and update the related edge's count*/
			    		  graphStore.deleteEdge(tmsg.getSecond(), tmsg.getThird());
			    		  lktruss.oneIteration(graphStore, graphStore.getEdge(tmsg.getSecond(), tmsg.getThird()), queue);
			    	  }
			      }
			  }
//			  LOG.info("After removing edges Superstep="+superstep);
//			  graphStore.dump();
			  
			  /*NOTE: process the subgraph, and send message */
			  if(!lktruss.iterativelyPrune(graphStore, queue)){
				 partitionStats.addFinishedVertexCount(graphStore.getVertexCount());
			  }
			  /* set partition stats here */
			  partitionStats.addVertexCount(graphStore.getVertexCount());
			  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  }
		  else if(superstep == 2){
			  /* generate the induced subgraph, initialize the queue */
			  for(RawVertex gv : graphStore.getVertexs()){
				  /* process message, generate induced subgraph */
				  I id = (I)new IntWritable(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages(id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
			    		  TripleWritable tmsg = (TripleWritable)msg;		    		  
			    		  /* add edge */
			    		  if(!graphStore.getVertex(tmsg.getSecond()).isLocal()
			    				  &&!graphStore.getVertex(tmsg.getThird()).isLocal()){
			    			  graphStore.addEdge(new RawEdge(tmsg.getSecond(), tmsg.getThird(), RawEdge.RAWEDGE_EXTERNAL));
			    		  }
			    	  }
			      }
			  }

//			  LOG.info("After adding edges Superstep="+superstep);
//			  graphStore.dump();
			  
			  /* remove insufficiently edges */
			  lktruss.initialize(graphStore, queue);
			  /* process the subgraph, and send message */
			  if(!lktruss.iterativelyPrune(graphStore, queue)){
				  /* no edge deleted and stop the algorithm */
				  partitionStats.addFinishedVertexCount(graphStore.getVertexCount());
			  }
			  
			  /* set partition stats here */
			  partitionStats.addVertexCount(graphStore.getVertexCount());
			  partitionStats.addEdgeCount(graphStore.getEdgeCount());
		  }
		  else if(superstep == 1){
			  /* answer the query from previous superstep */
			  IntWritable id = new IntWritable();
			  for(RawVertex gv : graphStore.getVertexs()){
				  /* process message, send reply message  */
				  id.set(gv.getId());
			      Iterable<M> messages = messageStore.getVertexMessages((I)id);
			      if (!Iterables.isEmpty(messages)) {
			    	  for(M msg : messages){
//			    		  LOG.info("msg: "+msg.toString());
			    		  TripleWritable tmsg = (TripleWritable)msg;
			    		  for(int vid : gv.getNeighbors()){
			    			  if(vid == tmsg.getThird()){
			    				  I tid = (I) new IntWritable(tmsg.getFirst());
			    				  M nmsg = (M) new TripleWritable(gv.getId(), tmsg.getSecond(), tmsg.getThird());
			    				  sendMessage(tid, nmsg);
			    				  
			    				  tid = (I) new IntWritable(tmsg.getSecond());
			    				  nmsg = (M) new TripleWritable(gv.getId(), tmsg.getFirst(), tmsg.getThird());
			    				  sendMessage(tid, nmsg);
			    				  
			    				  tid = (I) new IntWritable(tmsg.getThird());
			    				  nmsg = (M) new TripleWritable(gv.getId(), tmsg.getFirst(), tmsg.getSecond());
			    				  sendMessage(tid, nmsg);
			    			  }
			    		  }
			    	  }
			      }

				  /* collect data */
				  if(gv.isLocal()){
					  partitionStats.incrVertexCount();
					  partitionStats.addEdgeCount(gv.getNeighbors().size());
				  }
			  }
		  }
		  else if(superstep == 0){
			  /* 
			   * same as triangle count. 
			   * The input graph should be ordered
			   * TODO: NOTE: inorder to speed up the local vertex and remote vertex should be separated.
			   * */
			  for(RawVertex gv : graphStore.getVertexs()){

				  if(!gv.isLocal()) continue;
				  
				  /**
				   * Enumerate triads from the lowest degree vertex
				   */
				  for(int vid : gv.getNeighbors()){
					  for(int pvid : gv.getNeighbors()){
						  if(gv.getId() < vid
								  && gv.getId() < pvid
								  && vid < pvid){
							  /* send message */
							  I id = (I)new IntWritable(vid);
							  M msg = (M)new TripleWritable(gv.getId(), vid, pvid);
//							  LOG.info("superstep: 0 msg: "+msg.toString());
							  sendMessage(id, msg);
						  }
					  }
				  }
				  
				  /* collect data */
				  partitionStats.incrVertexCount();
				  partitionStats.addEdgeCount(gv.getNeighbors().size());
				  
			  }
		  }

//		  LOG.info("Out Superstep="+superstep);
//		  graphStore.dump();
	  }
	  
	  
	  private void sendMessage(I id, M message) {
	    if (graphState.getWorkerClientRequestProcessor().
	          sendMessageRequest(id, message)) {
	      graphState.getGraphTaskManager().notifySentMessages();
	    }
	}
	  
/**
 * This class manage the local ktruss algorithm
 * @author simon0227
 *
 */
	  public class LocalKTrussAlgorithm {
			
			public int threshold;
			private int deleteEdge = 0;
			
			public int getDeletedEdge(){
				return deleteEdge;
			}
			
			/**
			 * Need to remove isolated vertex here!
			 * @param graphStore
			 * @param queue
			 */
			public boolean iterativelyPrune(GraphStoreInterface graphStore, Queue<RawEdge> queue){
				if(queue.isEmpty()){
					graphStore.trim();
					return false;
				}
//				LOG.info("Beging Iteratively Pruning......");
				while(!queue.isEmpty()){
					RawEdge deleteEdge = queue.poll();
//					graphStore.getEdge(deleteEdge.getSmallEndPoint(), deleteEdge.getBigEndPoint()).delete();
					deleteEdge.delete();
					oneIteration(graphStore, deleteEdge, queue);
				}
				graphStore.trim();
				return true;
			}
			
			/**
			 * Consider the deleted edge's influence,
			 * when it is eliminated from the queue.
			 * @param graphStore
			 * @param queue
			 */
			public void initialize(GraphStoreInterface graphStore, Queue<RawEdge> queue){
				int externalEdge = 0;
				for(RawEdge edge : graphStore.getEdges()){
					/* do not process external edge */
					if(edge.isExternal()){
						externalEdge++;
						continue;
					}
					
					ArrayList<Integer> nlist1 = graphStore.getVertex(edge.getSmallEndPoint()).getNeighbors();
					ArrayList<Integer> nlist2 = graphStore.getVertex(edge.getBigEndPoint()).getNeighbors();
					
					int count = 0;
					for(int vid : nlist1){
						if(nlist2.contains(vid)){
							count++;
						}
					}
					edge.setCount(count);
					if(count < threshold){
						queue.add(edge);
						this.deleteEdge++;
					}
//					if(LOG.isInfoEnabled()){
//						LOG.info("After Initialization: RawEdge "+ edge.toString());
//					}
				}
				LOG.info("External Edge="+externalEdge);
			}
			
			/**
			 * 1. update neighbored edges count, if they are internal and cross
			 * 2. send message to the vertex, if the deleted edge has a common neighbor who is remote. 
			 * @param graphStore
			 * @param deleteEdge
			 * @param queue
			 */
			public void oneIteration(GraphStoreInterface graphStore, RawEdge deleteEdge, Queue<RawEdge> queue){
				int firstEndPoint = deleteEdge.getSmallEndPoint();
				int secondEndPoint = deleteEdge.getBigEndPoint();
				ArrayList<Integer> nlist1 = graphStore.getVertex(firstEndPoint).getNeighbors();
				ArrayList<Integer> nlist2 = graphStore.getVertex(secondEndPoint).getNeighbors();

				for(int vid : nlist1){
					/**
					 * We should assure that the triangle is still exist.
					 */
					if(nlist2.contains(vid) 
							&& !graphStore.getEdge(firstEndPoint, vid).isDelete()
							&& !graphStore.getEdge(secondEndPoint, vid).isDelete()){
						/* 1. update neighbor */
//						if(graphStore.getVertex(firstEndPoint).isLocal() || graphStore.getVertex(vid).isLocal()){
						if(!graphStore.getEdge(firstEndPoint, vid).isExternal()){
							/**
							 * Because we make sure the edge support count decrease one by one.
							 */
							if(graphStore.getEdge(firstEndPoint, vid).decAndGetCount() + 1 == threshold){
//								graphStore.getEdge(firstEndPoint, vid).delete();
								this.deleteEdge++;
								queue.add(graphStore.getEdge(firstEndPoint, vid));
							}
						}
//						if(graphStore.getVertex(secondEndPoint).isLocal() || graphStore.getVertex(vid).isLocal()){
						if(!graphStore.getEdge(secondEndPoint, vid).isExternal()){
							if(graphStore.getEdge(secondEndPoint, vid).decAndGetCount() + 1 ==  threshold){
//								graphStore.getEdge(secondEndPoint, vid).delete();
								queue.add(graphStore.getEdge(secondEndPoint, vid));
								this.deleteEdge++;
							}
						}
						
						/* 2. track notify list */
						if(!graphStore.getVertex(vid).isLocal()
								&& !graphStore.isSamePartition(firstEndPoint, vid)
								&& !graphStore.isSamePartition(secondEndPoint, vid)){
							I id = (I) new IntWritable(vid);
							M msg = (M)new TripleWritable(firstEndPoint, firstEndPoint, secondEndPoint);
//							LOG.info("Send remove message: "+msg+" to"+vid);
							sendMessage(id, msg);
						}
					}
				}
			}
		}
}
