package org.apache.dstream.tez;

import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class TezDelegatingPartitioner extends HashPartitioner {
	private static Partitioner delegatorPartitioner;
	
	public static void setDelegator(Partitioner partitioner){
		delegatorPartitioner = partitioner;
	}

	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		if (delegatorPartitioner != null){
			return delegatorPartitioner.getPartition(key, value, numPartitions);
		} else {
			return super.getPartition(key, value, numPartitions);
		}
	}
}
