package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.Map.Entry;

public abstract class Partitioner<K,V> implements Serializable {

	private static final long serialVersionUID = -3799649258371438298L;

	private final int partitionSize;
	
	public Partitioner(int partitionSize){
		this.partitionSize = partitionSize;
	}
	
	public abstract int getPartition(Entry<K,V> input);
	
	public int getPartitionSize() {
		return partitionSize;
	}
}
