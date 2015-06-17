package org.apache.dstream.tez;

import org.apache.dstream.Splitter;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class TezDelegatingPartitioner extends HashPartitioner {
	private static Splitter<? super Object> delegatorPartitioner;
	
	public static void setDelegator(Splitter<? super Object> splitter){
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
