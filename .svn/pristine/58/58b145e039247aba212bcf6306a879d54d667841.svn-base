package org.apache.giraph.tools.graphanalytics.simrank;

import org.apache.giraph.tools.utils.HashMapWritable;
import org.apache.giraph.worker.WorkerContext;
import org.mortbay.log.Log;

public class SimRankWorkerContext extends WorkerContext {
	
	private HashMapWritable localSimRank;
	private int MAX_STEPS = -1;

	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
		MAX_STEPS = this.getContext().getConfiguration().getInt("simrank.maxiter", 11);
		localSimRank = new HashMapWritable();
	}

	@Override
	public void postApplication() {
		// TODO Auto-generated method stub

	}

	@Override
	public void preSuperstep() {
		// TODO Auto-generated method stub

	}

	@Override
	public void postSuperstep() {
		System.out.println("Superstep "+this.getSuperstep()+" HashMap for localSimRank size = "+localSimRank.size());
		if(this.getSuperstep() == MAX_STEPS){
			this.aggregate("simrank.localagg", localSimRank);
		}
	}
	
	public void add(int vid, double simrank){
//		System.out.println("Superstep "+this.getSuperstep()+" vid = "+ vid+" SimRank = "+String.format("%.9f", simrank));
		localSimRank.add(vid, simrank);
	}

}
