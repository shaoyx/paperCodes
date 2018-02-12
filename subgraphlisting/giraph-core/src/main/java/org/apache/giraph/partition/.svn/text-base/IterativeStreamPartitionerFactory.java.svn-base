package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class IterativeStreamPartitionerFactory <I extends WritableComparable, 
V extends Writable, E extends Writable, M extends Writable>
implements GraphPartitionerFactory<I, V, E, M> {

	/** Saved configuration */
	private ImmutableClassesGiraphConfiguration conf;

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration configuration) {
		this.conf = configuration;
	}
	
	@Override
	public ImmutableClassesGiraphConfiguration getConf() {
		return conf;
	}

	@Override
	public MasterGraphPartitioner<I, V, E, M> createMasterGraphPartitioner() {
		return new IterativeStreamMasterPartitioner(conf);
	}

	@Override
	public WorkerGraphPartitioner<I, V, E, M> createWorkerGraphPartitioner() {
		return new IterativeStreamWorkerPartitioner(conf);
	}

}
