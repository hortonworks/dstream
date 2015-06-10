package org.apache.dstream.tez;

import org.apache.dstream.support.Parallelizer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class TezDelegatingPartitioner extends HashPartitioner {
	private static Parallelizer<? super Object> delegatorPartitioner;
	
	public static void setDelegator(Parallelizer<? super Object> parallelizer){
		delegatorPartitioner = parallelizer;
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
