package org.apache.giraph.graph;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class Monitor {
	
	private static Logger LOG = Logger.getLogger(Monitor.class);
	
	private long superstepTime = Long.MAX_VALUE;
	private boolean superstepChanged = false;
	
//	private double edgeCutRatio;
	
	/* metrics related message genetaion(workload generation) */
//	private int generatedMessage;
//	private double generationCost;
//	private double sg;
	
	private int localIncomingMsg;
	private long localIncomingMsgTime;
	private double sl;
	private int nlmp;
	private long localMsgBlockingTime = Long.MAX_VALUE;
	boolean localChanged = false;
	
	private int remoteIncomingMsg;
	private long remoteIncomingMsgTime;
	private double sr;
	private int nrmp;
	private long remoteMsgBlockingTime = Long.MAX_VALUE;
	boolean remoteChanged = false;
	
	/* metrics related to the message processing */
	private int totalProcessingMsg;
	private long totalProcessingMsgTime;
//	private int nmp;
	/**
	 * sp = 0.03158932627742692
	 * when each thread process small number of requests, 
	 * the tracked time is inaccurate.
	 */
	private double sp;

	private long syncLocalCost;

	private long syncRemoteCost; 
	
	public Monitor(){
		
	}
	
	public Monitor(Configuration conf){
		nlmp = conf.getInt(GiraphConstants.GIRAPH_AYSNCLOCAL_CONCURRENCY, 
				GiraphConstants.DEFAULT_GIRAPH_AYSNCLOCAL_CONCURRENCY);
		nrmp = conf.getInt(GiraphConstants.NETTY_SERVER_EXECUTION_THREADS, 
				GiraphConstants.NETTY_SERVER_EXECUTION_THREADS_DEFAULT);
		LOG.info("initial NLMP="+nlmp+" NRMP="+nrmp);
	}
	
	public void setSuperstepTime(long superstep, long time){
		/**
		 * TODO: now only fit for the always active model.
		 */
//		if(superstep != 0) {
			superstepChanged = (superstepTime - time > 3000);
//		}
		superstepTime = time;
		LOG.info("SuperstepTime="+this.superstepTime);
	}
	
	public void setSLMetrics(int msgNum, long time){
		localIncomingMsg = msgNum;
		localIncomingMsgTime = time;
		sl = localIncomingMsg * 1.0 / localIncomingMsgTime;
	}
	
	public void setSRMetrics(int msgNum, long time){
		remoteIncomingMsg = msgNum;
		remoteIncomingMsgTime = time;
		sr = remoteIncomingMsg * 1.0 / remoteIncomingMsgTime;
	}
	
	public void setSPMetrics(int msgNum, long time){
		totalProcessingMsg = msgNum;
		totalProcessingMsgTime = time;
		sp = totalProcessingMsg * 1.0 / totalProcessingMsgTime;
	}
	
	public int getNlmp(){
//		return (int) Math.round(sl/sp + 0.5);
		return nlmp;
	}
	
	public int getNrmp(){
//		return (int) Math.round(sr/sp + 0.5);
		return nrmp;
	}
	
	public String toString(){
//		int nmp = (int) Math.round((sl+sr)/sp + 0s.5);
//		int nlmp = (int) Math.round(sl/sp + 0.5);
//		int nrmp = (int) Math.round(sr/sp + 0.5);
		int nmp = nlmp+nrmp;
//		return "\nTotalMsg = "+totalProcessingMsg
//				+" TotalMsgTime = "+totalProcessingMsgTime
//				+" SP= "+ sp
//				+" NMP = "+nmp+"\n"
//				+"LocalMsg = "+localIncomingMsg
//				+" LocalMsgTime = " + localIncomingMsgTime
//				+" SL= "+sl
//				+" NLMP="+nlmp+"\n"
//				+"RemoteMsg= "+remoteIncomingMsg
//				+" RemoteMsgTime= "+remoteIncomingMsgTime
//				+" SR= "+sr
//				+" NRMP="+nrmp;
		return "\nNMP="+nmp+"\nNLMP="+nlmp+"\nNRMP="+nrmp;
	}

	/**
	 * according to the threadhold to change the nrmp
	 * @param blockingTime
	 */
	public void setSRMetrics(long blockingTime){
		remoteChanged = (blockingTime > 0 && remoteMsgBlockingTime - blockingTime > 1000);
		remoteMsgBlockingTime = blockingTime;
		LOG.info("Remote Blocking Time="+blockingTime);
	}

	/**
	 * according to the threadheld to change the nlmp
	 * @param blockingTime
	 */
	public void setSLMetrics(long blockingTime) {
		localChanged = (blockingTime > 0 && localMsgBlockingTime - blockingTime > 1000);
		localMsgBlockingTime = blockingTime;
		LOG.info("Local Blocking Time="+blockingTime);
	}

	public void setWaitForBlockingQueue(long waitForBlockingQueue, long compTime) {
		syncLocalCost = waitForBlockingQueue;
		double delta = nlmp == 1 ? .5 : .0;
//		if(syncLocalCost > compTime * .3 && localMsgBlockingTime > 1000){ //1000ms
		/**
		 * Here localMsgBlockingTime vs. syncLocalCost
		 */
//		if(localMsgBlockingTime > 1000){
		if(syncLocalCost > 0) {// && localChanged){
//			nlmp = (int) (nlmp *(1 + Math.round(syncLocalCost * 1.0 / compTime + .5)));
			nlmp = (int) Math.round(nlmp *(1 + syncLocalCost * 1.0 / compTime + delta));
//			nlmp = (int) Math.round(nlmp *(1 + localMsgBlockingTime * 1.0 / compTime + delta));
			LOG.info("syncLocalCost="+waitForBlockingQueue+
					" totalMsgTime="+compTime+
					" ratio="+(syncLocalCost * 1.0 / compTime+.2)+
					" nlmp="+nlmp);
		}
	}

	public void setWaitForRequest(long waitForRequestCost, long compTime) {
		syncRemoteCost = waitForRequestCost;
		double delta = nrmp == 1 ? .5 : .0;
//		if(syncRemoteCost > compTime * .3 && remoteMsgBlockingTime > 1000) {//thresdhold=1000ms
		/**
		 * Here remoteMsgBlockingTime vs. syncRemoteCost. no relation?
		 * if aggeregate is small, means it is the slower one
		 * then if it is stuck on the wait blocking
		 */
//		if( remoteMsgBlockingTime > 1000){
		if(remoteChanged && superstepChanged && syncRemoteCost > 0){
//			nrmp = (int) (nrmp + Math.round(syncRemoteCost * 1.0 / compTime + .5));
//			nrmp = (int) (nrmp *(1 + Math.round(syncRemoteCost * 1.0 / compTime + .5)));
			nrmp = (int) Math.round(nrmp *(1 + syncRemoteCost * 1.0 / compTime + delta));
//			nrmp = (int) Math.round(nrmp *(1 + remoteMsgBlockingTime * 1.0 / compTime + delta));
			LOG.info("syncRemoteCost="+waitForRequestCost+
					" totalMsgTime="+compTime+
					" ratio="+(syncRemoteCost * 1.0 / compTime + .2)+
					" nrmp="+nrmp);
		}
//		LOG.info("syncRemoteCost="+waitForRequestCost);
	}
}
