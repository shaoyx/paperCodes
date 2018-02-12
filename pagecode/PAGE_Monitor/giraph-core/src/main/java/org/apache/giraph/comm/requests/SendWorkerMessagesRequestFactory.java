package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.netty.NettyWorkerServerWithBlockQueueThread;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class SendWorkerMessagesRequestFactory<I extends WritableComparable,
M extends Writable> {
	
	private final static Logger LOG = 
		    Logger.getLogger(SendWorkerMessagesRequestFactory.class);
	
	private boolean customRequest = false;
	
//	public SendWorkerMessagesRequestFactory(Configuration conf){
//		this.conf = conf;
//	}
	
	public void setRequestType(Configuration conf){
		customRequest= ("queue".equals(conf.get(GiraphConstants.GIRAPH_ENGINE, GiraphConstants.DEFAULT_GIRAPH_ENGINE)) ||
				"queuethread".equals(conf.get(GiraphConstants.GIRAPH_ENGINE, GiraphConstants.DEFAULT_GIRAPH_ENGINE)));
		
		LOG.info("Using customRequest=" + customRequest);
	}
	
	public WritableRequest createSendWorkerMessagesRequest(Configuration conf, 
			PairList<Integer, ByteArrayVertexIdMessages<I, M>> workerMessages){
//		System.out.println("Engine Info: "+ conf.get(GiraphConstants.GIRAPH_ENGINE, GiraphConstants.DEFAULT_GIRAPH_ENGINE));
		if(customRequest){
			return new SendWorkerMessagesRequestWithBuffered(workerMessages);
		}
		else{
			return new SendWorkerMessagesRequest(workerMessages);
		}
	}

}
