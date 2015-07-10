package org.apache.dstream.tez;

import org.apache.dstream.Partitioner;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class TezDelegatingPartitioner extends HashPartitioner {
	private static Partitioner<? super Object> delegatorPartitioner;
	
	public static void setDelegator(Partitioner<? super Object> splitter){
		delegatorPartitioner = splitter;
	}

	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		if (delegatorPartitioner != null){
			return delegatorPartitioner.apply(key);
		} 
		else {
			return super.getPartition(key, value, numPartitions);
		}
	}
}
