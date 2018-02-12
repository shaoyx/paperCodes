package org.apache.giraph.comm.netty;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IncomingMessagesReceiver<I extends WritableComparable,
V extends Writable, E extends Writable, M extends Writable>
extends Thread {
	/** Server data storage */
	private final ServerData<I, V, E, M> serverData;
	private final BlockingQueue<PairList<Integer, ByteArrayVertexIdMessages<I, M>>> recievedMessagesForNextSuperstepBuffer;
	private long costtime = 0;
	private long waittime = 0;
	private int requestCount = 0;
	public IncomingMessagesReceiver(BlockingQueue<PairList<Integer, ByteArrayVertexIdMessages<I, M>>> buffer,
			ServerData<I,V,E,M> serverdata){
		this.recievedMessagesForNextSuperstepBuffer = buffer;
		this.serverData = serverdata;
	}
	
	public void run(){
		PairList<Integer, ByteArrayVertexIdMessages<I,M>> recievedMessage;
		long start_time;
		while (true) {
			try {
				start_time = System.currentTimeMillis();
				recievedMessage = recievedMessagesForNextSuperstepBuffer.take();
				waittime += System.currentTimeMillis() - start_time;
			    PairList<Integer, ByteArrayVertexIdMessages<I, M>>.Iterator
			        iterator = recievedMessage.getIterator();
			    
			    start_time = System.currentTimeMillis();
			    while (iterator.hasNext()) {
			      iterator.next();
			      serverData.getIncomingMessageStore().
			      addPartitionMessages(iterator.getCurrentFirst(), iterator.getCurrentSecond());
			    }
			    requestCount++;
			    costtime += System.currentTimeMillis() - start_time;
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean hasFinished(){
		return (recievedMessagesForNextSuperstepBuffer.peek() == null);
	}
	
	public long getMessageProcessCost(){
		long result = costtime;
		costtime = 0;
		return result;
	}
	
	public long getWaitTime(){
		long result = waittime;
		waittime = 0;
		return result;
	}
	
	public int getRequestCount(){
		int result = requestCount;
		requestCount = 0;
		return result;
	}
}
