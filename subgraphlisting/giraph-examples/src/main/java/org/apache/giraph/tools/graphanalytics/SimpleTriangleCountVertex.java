package org.apache.giraph.tools.graphanalytics;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.tools.utils.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class SimpleTriangleCountVertex extends  
Vertex<IntWritable, IntWritable, NullWritable, ArrayWritable> {
	  /** Number of supersteps for this test */
	  public static final int MAX_SUPERSTEPS = 10;
	  /** Logger */
	  private static final Logger LOG =
	      Logger.getLogger(SimpleTriangleCountVertex.class);

	  private List<Integer> neighborList = Lists.newArrayList();
	  
	  /**
	   * Triangle Counting algorithm:
	   * Step 1: node u sends its neighbor list to each of its neighbor
	   * Step 2: node u counts each edge's triangle  
	   */
	  @SuppressWarnings("unchecked")
	@Override
	  public void compute(Iterable<ArrayWritable> messages) {
		  //voteToHalt();
		  /* send neighborlist */
		  if(getSuperstep() == 0) {
			  for( Edge<IntWritable, NullWritable> edge : getEdges()){
//				  if(getId().get() == 2108608){
//					  System.out.println("vid="+getId().get()+": neighbor="+edge.getTargetVertexId().get());
//				  }
				  neighborList.add(edge.getTargetVertexId().get());
			  }
//			  for(int i = 0; i < neighborList.size(); i++){
//				  System.out.print(neighborList.get(i)+" ");
//			  }
//			  System.out.println();
			  Collections.sort(neighborList);

//			  for(int i = 0; i < message.length; i++){
//				  message[i] = new IntWritable(neighborList.get(i).get());
//				  System.out.print(message[i]+" ");
//			  }
//			  System.out.println();
//			  for(int i = 0; i < neighborList.size(); i++){
//				  System.out.print(neighborList.get(i)+" ");
//			  }
//			  System.out.println();
			  sendMessageToAllEdges(new ArrayWritable(neighborList));
		  }
		  /* counting triangle and voteToHalt*/
		  else if(getSuperstep() == 1) {
			  int count = 0;
			  
//			  if(getId().get() == 2108608){
//				  for(int i = 0; i < neighborList.size(); i++){
//					  System.out.print(neighborList.get(i)+" ");
//				  }
//				  System.out.println();
//			  }
			  
			  for (ArrayWritable message : messages) {
		    	  int nidx = 0;
		    	  int midx = 0;
		    	  /*We are sure that the it is a Integer Writable array */
		    	  List<Integer> msg = message.get();
		    	  
		    	  /* check valid */
//		    	  for(nidx = 1; nidx < msg.size(); nidx++){
//		    		  if(msg.get(nidx -1) > msg.get(nidx)){
//		    			  System.out.println("Failed To check msg");
//		    			  System.exit(1);
//		    		  }
//		    	  }
//		    	  System.out.println();

//				  if(getId().get() == 2108608){
//					  System.out.println("Message:");
//					  for(int i = 0; i < msg.size(); i++){
//						  System.out.print(msg.get(i)+" ");
//					  }
//					  System.out.println();
//				  }
				  
		    	  nidx = 0;
		    	  midx = 0;
		    	  for(; nidx < neighborList.size(); nidx++) {
		    		  for(; midx < msg.size(); midx++){
//		    			  if(neighborList.get(nidx) == msg.get(midx)){
		    			  if(neighborList.get(nidx).equals(msg.get(midx))){
//		    				  if(getId().get() == 2108608){
//		    					  System.out.println("Pair: ("+nidx+","+midx+") ==> (" + neighborList.get(nidx)+", "+msg.get(midx)+")");
//		    				  }
		    				  count++;
		    			  }
		    			  else if(neighborList.get(nidx) < msg.get(midx)){
//		    				  if(getId().get() == 2108608){
//		    					  System.out.println("Break point: ("+nidx+","+midx+") ==> (" + neighborList.get(nidx)+", "+msg.get(midx)+")");
//		    				  }
		    				  break;
		    			  }
		    		  }
		    	  }
		      }

//			  if(getId().get() == 2108608){
//				  System.out.println("Count = "+ count);
//			  }
		      /*set to the vertex*/
		      this.setValue(new IntWritable(count));
		      voteToHalt();			  
		  }
	  }
	  
	  /** Vertex InputFormat */
	  public static class SimpleTriangleCountVertexInputFormat extends
	  AdjacencyListTextVertexInputFormat<IntWritable, IntWritable, NullWritable, ArrayWritable> {
		/** Separator for id and value */
		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

		@Override
		public AdjacencyListTextVertexReader createVertexReader(
				InputSplit split, TaskAttemptContext context) {
			return new SimpleTriangleCountReader();
		}
		
		public class  SimpleTriangleCountReader extends AdjacencyListTextVertexReader {
			
			protected String[] preprocessLine(Text line) throws IOException {
				String[] values = SEPARATOR.split(line.toString());
				return values;
			}

			    @Override
			    protected IntWritable getId(String[] values) throws IOException {
			      return decodeId(values[0]);
			    }


			    @Override
			    protected IntWritable getValue(String[] values) throws IOException {
			      return decodeValue(null);
			    }

			    @Override
			    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(String[] values) throws
			        IOException {
			      int i = 1;
			      List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
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
			public IntWritable decodeValue(String s) {
				return new IntWritable(0);
			}

			@Override
			public Edge<IntWritable, NullWritable> decodeEdge(String id,
					String value) {
				return EdgeFactory.create(decodeId(id), NullWritable.get());
			}
		}
	  }
	  
	  /**
	   * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
	   */
	  public static class SimpleTriangleCountVertexOutputFormat extends
	      TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {
	    @Override
	    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
	      throws IOException, InterruptedException {
	      return new SimpleTriangleCountVertexWriter();
	    }

	    /**
	     * Simple VertexWriter that supports {@link SimplePageRankVertex}
	     */
	    public class SimpleTriangleCountVertexWriter extends TextVertexWriter {
	      @Override
	      public void writeVertex(
	          Vertex<IntWritable, IntWritable, NullWritable, ?> vertex)
	        throws IOException, InterruptedException {
	        getRecordWriter().write(
	            new Text(vertex.getId().toString()),
	            new Text(vertex.getValue().toString()));
	      }
	    }
	  }

}
