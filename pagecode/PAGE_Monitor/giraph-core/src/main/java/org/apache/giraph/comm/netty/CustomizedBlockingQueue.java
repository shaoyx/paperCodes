package org.apache.giraph.comm.netty;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CustomizedBlockingQueue<E> extends LinkedBlockingQueue<E> {
	private AtomicLong blockingTime = new AtomicLong(0);
	private AtomicLong startTime = new AtomicLong(Long.MAX_VALUE);
	private AtomicLong endTime = new AtomicLong(Long.MIN_VALUE);
	private AtomicLong incomingMsg = new AtomicLong(0);

//	public void put(E e) throws InterruptedException{
////		long time = System.currentTimeMillis();
////		if(startTime.get() > time) startTime.set(time);
////		if(endTime.get() < time) endTime.set(time);
////		System.out.println("time="+time+" start="+startTime.get()+" end="+endTime.get());
////		incomingMsg.addAndGet(1);
////		System.out.println("In put: "+e.toString());
//		super.put(e);
//	}
	
//	public boolean offer(E e){
//		long time = System.currentTimeMillis();
//		if(startTime.get() > time) startTime.set(time);
//		if(endTime.get() < time) endTime.set(time);
//		incomingMsg.addAndGet(1);
////		System.out.println("In offer1: "+e.toString());
//		return super.offer(e);
//	}
	
//	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException{
////		long time = System.currentTimeMillis();
////		if(startTime.get() > time) startTime.set(time);
////		if(endTime.get() < time) endTime.set(time);
////		incomingMsg.addAndGet(1);
////		System.out.println("In offer2: "+e.toString());
//		return super.offer(e, timeout, unit);
//	}
	
	public E peek(){
		checkBlocking();
		return super.peek();
	}
	
	public E poll(){
		checkBlocking();
		return super.poll();
	}
	public E poll(long timeout, TimeUnit unit) throws InterruptedException{
		checkBlocking();
		return super.poll(timeout, unit);
	}
	public E take() throws InterruptedException{
		checkBlocking();
		return super.take();
	}
	
	private void checkBlocking(){
		int size = this.size();
		if(size > 1){
			long time = System.currentTimeMillis();
			if(startTime.get() > time) startTime.set(time);
			if(endTime.get() < time) endTime.set(time);
		}
		else{
			if(startTime.get() != Long.MAX_VALUE){
				blockingTime.addAndGet(endTime.get() - startTime.get());
				startTime.set(Long.MAX_VALUE);
				endTime.set(Long.MIN_VALUE);
			}
		}
	}
	
	
  public void resetTime(){
//	  System.out.println("Reset: IntervalTime="+(endTime.get() - startTime.get())
//			  +" start="+startTime.get()+" end="+endTime.get()+" msg="+incomingMsg.get());
//	  System.out.println();
	  incomingMsg.set(0);
	  blockingTime.set(0);
	  startTime.set(Long.MAX_VALUE);
	  endTime.set(Long.MIN_VALUE);
  }
  
  public long getTimeInterval(){
//	  System.out.println("Get: IntervalTime="+(endTime.get() - startTime.get())+" start="
//  +startTime.get()+" end="+endTime.get()+" msg="+incomingMsg.get());
//      System.out.println();
	  return endTime.get() - startTime.get();
  }
  
  public long getBlockingTime(){
		if(startTime.get() != Long.MAX_VALUE){
			blockingTime.addAndGet(endTime.get() - startTime.get());
			startTime.set(Long.MAX_VALUE);
			endTime.set(Long.MIN_VALUE);
		}
	  return blockingTime.get();
  }
}
