package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.requests.RequestType;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

public class RemoteTimeTracker extends OneToOneDecoder {
	  
	  /* tracking arrival time */
	  private long startTime = Long.MAX_VALUE;
	  private long endTime = Long.MIN_VALUE;

	  @Override
	  protected Object decode(ChannelHandlerContext ctx,
	      Channel channel, Object msg) throws Exception {
	    if (!(msg instanceof ChannelBuffer)) {
	      throw new IllegalStateException("decode: Got illegal message " + msg);
	    }

	    // Decode the request
	    ChannelBuffer buffer = (ChannelBuffer) msg;
	    int enumValue = buffer.getByte(0);
	    
	    if(RequestType.SEND_WORKER_MESSAGES_REQUEST.ordinal() == enumValue){
	    	long time = System.currentTimeMillis();
	    	if(time < startTime) startTime = time;
	    	if(time > endTime) endTime = time;
	    }

	    return msg;
	  }
	  
	  public void resetTime(){
		  startTime = Long.MAX_VALUE;
		  endTime = Long.MIN_VALUE;
	  }
	  
	  public long getTimeInterval(){
		  return endTime - startTime;
	  }

}
