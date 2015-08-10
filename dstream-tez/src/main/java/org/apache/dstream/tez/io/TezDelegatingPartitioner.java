package org.apache.dstream.tez.io;

import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import dstream.function.GroupingFunction;

public class TezDelegatingPartitioner extends HashPartitioner {
	
	private static GroupingFunction delegatorPartitioner;
	
	public static void setDelegator(GroupingFunction partitioner){
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
