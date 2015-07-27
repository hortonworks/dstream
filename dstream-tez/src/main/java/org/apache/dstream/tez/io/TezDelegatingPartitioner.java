package org.apache.dstream.tez.io;

import org.apache.dstream.support.Partitioner;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class TezDelegatingPartitioner extends HashPartitioner {
	private static Partitioner<? super Object> delegatorPartitioner;
	
	public static void setDelegator(Partitioner<? super Object> partitioner){
		delegatorPartitioner = partitioner;
	}

	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		return this.doGetPartition((KeyWritable)key, (ValueWritable<?>)value, numPartitions);
	}
	
	public int doGetPartition(KeyWritable key, ValueWritable<?> value, int numPartitions) {
		int partitionId;
		Object valueToUse = key;
		if (key.getValue() == null){
			valueToUse = value.getValue();
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
