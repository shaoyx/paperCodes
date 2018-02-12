package org.apache.giraph.worker;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.requests.WorkerRequest;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

class RequestTask <I extends WritableComparable,
V extends Writable, E extends Writable, M extends Writable>
implements Runnable{
	private ServerData<I,V,E,M> serverData;
	private WorkerRequest wr;
	
	public RequestTask(WorkerRequest wr, ServerData<I,V,E,M> serverdata){
		this.wr = wr;
		this.serverData = serverdata;
	}
	
	@Override
	public void run() {
		wr.doRequest(serverData);
	}	
}