package org.apache.dstream.tez.io;

import org.apache.dstream.function.PartitionerFunction;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class TezDelegatingPartitioner extends HashPartitioner {
	private static PartitionerFunction<? super Object> delegatorPartitioner;
	
	public static void setDelegator(PartitionerFunction<? super Object> partitioner){
		delegatorPartitioner = partitioner;
	}

	/**
	 * 
	 */
	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		return this.doGetPartition((KeyWritable)key, (ValueWritable<?>)value, numPartitions);
	}
	
	/**
	 * 
	 */
	private int doGetPartition(KeyWritable key, ValueWritable<?> value, int numPartitions) {
		int partitionId;
		Object valueToUse = key;
		if (key.getValue() == null){
			valueToUse = value.getValue();
		}
		else {
			valueToUse = key.getValue();
		}
		if (delegatorPartitioner != null){
			partitionId = delegatorPartitioner.apply(valueToUse);
		} 
		else {
			partitionId = super.getPartition(valueToUse, null, numPartitions);
		}
		return partitionId;
	}
}
