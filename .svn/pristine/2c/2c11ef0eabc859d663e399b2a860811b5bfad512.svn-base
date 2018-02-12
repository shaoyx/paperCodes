package org.apache.giraph.comm.messages;

import com.google.common.collect.Iterators;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.RepresentativeByteArrayIterable;
import org.apache.giraph.utils.RepresentativeByteArrayIterator;
import org.apache.giraph.utils.VertexIdIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * The implementation is optimized for storing messages of
 * path evaluation.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class CompactMessagesPerVertexStore<I extends WritableComparable,
    M extends Writable> extends SimpleMessageStore<I, M, VertexMessageStore> {
  /**
   * Constructor
   *
   * @param service Service worker
   * @param config Hadoop configuration
   */
  public CompactMessagesPerVertexStore(
      CentralizedServiceWorker<I, ?, ?, M> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
    super(service, config);
  }

  /**
   * Factory for {@link CompactMessagesPerVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
    /** Service worker */
    private final CentralizedServiceWorker<I, ?, ?, M> service;
    /** Hadoop configuration */
    private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> config;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, ?, ?, M> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?, M> config) {
      this.service = service;
      this.config = config;
    }

    @Override
    public MessageStoreByPartition<I, M> newStore() {
      return new CompactMessagesPerVertexStore(service, config);
    }
  }

  @Override
  public VertexMessageStore getVertexMessageStore(I vid) {
	ConcurrentMap<I, VertexMessageStore> partitionMap =  map.get(getPartitionId(vid));
	if(partitionMap == null)
		return null;
	return partitionMap.get(vid);
  }
	
	@Override
	public void addPartitionMessages(int partitionId,
			ByteArrayVertexIdMessages<I, M> messages) throws IOException {
	    ConcurrentMap<I, VertexMessageStore> partitionMap =
	            getOrCreatePartitionMap(partitionId);
	    
	        ByteArrayVertexIdMessages<I, M>.VertexIdMessageBytesIterator
	            vertexIdMessageBytesIterator =
	            messages.getVertexIdMessageBytesIterator();
	        // Try to copy the message buffer over rather than
	        // doing a deserialization of a message just to know its size.  This
	        // should be more efficient for complex objects where serialization is
	        // expensive.  If this type of iterator is not available, fall back to
	        // deserializing/serializing the messages
	        if (vertexIdMessageBytesIterator != null) {
	          while (vertexIdMessageBytesIterator.hasNext()) {
	            vertexIdMessageBytesIterator.next();
	          }
	        } else {
	          ByteArrayVertexIdMessages<I, M>.VertexIdMessageIterator
	              vertexIdMessageIterator = messages.getVertexIdMessageIterator();
	          while (vertexIdMessageIterator.hasNext()) {
	            vertexIdMessageIterator.next();
	            I vid = vertexIdMessageIterator.getCurrentVertexId();
	            VertexMessageStore vms = partitionMap.get(vid);
	            if(vms == null) {
	            	VertexMessageStore nvms = new VertexMessageStore();
	            	vms = partitionMap.putIfAbsent(vid, new VertexMessageStore());
	            	if(vms == null) {
	            		vms = nvms;
	            	}
	            }
	            vertexIdMessageIterator.getCurrentMessage();
	            //vms.addMessage();
	          }
	        }
	}
	
	@Override
	public void addMessages(MessageStore<I, M> messageStore) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected Iterable<M> getMessagesAsIterable(VertexMessageStore messages) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	protected int getNumberOfMessagesIn(
			ConcurrentMap<I, VertexMessageStore> partitionMap) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	protected void writeMessages(VertexMessageStore messages, DataOutput out)
			throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected VertexMessageStore readFieldsForMessages(DataInput in)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
