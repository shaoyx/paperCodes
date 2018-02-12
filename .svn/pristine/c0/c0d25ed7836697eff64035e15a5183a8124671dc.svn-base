package org.apache.giraph.worker;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.zk.BspEvent;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface IBspServiceWorker<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> 
extends CentralizedServiceWorker<I, V, E, M>{

	/** Name of gauge for time spent waiting on other workers */
	public static final String TIMER_WAIT_REQUESTS = "wait-requests-us";

	/**
	 * Intended to check the health of the node.  For instance, can it ssh,
	 * dmesg, etc. For now, does nothing.
	 * TODO: Make this check configurable by the user (i.e. search dmesg for
	 * problems).
	 *
	 * @return True if healthy (always in this case).
	 */
	public boolean isHealthy();

	/**
	 * Get event when the state of a partition exchange has changed.
	 *
	 * @return Event to check.
	 */
	public BspEvent getPartitionExchangeChildrenChangedEvent();

}