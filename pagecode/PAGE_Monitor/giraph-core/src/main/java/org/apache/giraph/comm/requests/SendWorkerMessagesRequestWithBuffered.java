package org.apache.giraph.comm.requests;

import java.util.concurrent.BlockingQueue;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Send a collection of vertex messages for a partition.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerMessagesRequestWithBuffered<I extends WritableComparable,
    M extends Writable>
    extends SendWorkerMessagesRequest<I, M> {
  /**
   * Constructor used for reflection only
   */
  public SendWorkerMessagesRequestWithBuffered() { }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgs Map of remote partitions =>
   *                     ByteArrayVertexIdMessages
   */
  public SendWorkerMessagesRequestWithBuffered(
      PairList<Integer, ByteArrayVertexIdMessages<I, M>> partVertMsgs) {
	  super(partVertMsgs);
  }
  
  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST_WITH_BUFFERED;
  }
  
  @Override
  public void doRequest(ServerData serverData) {
      try {
    	  BlockingQueue<PairList<Integer, ByteArrayVertexIdMessages<I, M>>> buffer = serverData.getIncomingMessageBuffer();
    	  buffer.put(partitionVertexData);
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
  }
}
