package org.apache.dstream.utils;

import java.io.Serializable;

public abstract class Partitioner<T> implements Serializable {

	private static final long serialVersionUID = -3799649258371438298L;

	private final int partitionSize;
	
	public Partitioner(int partitionSize){
		this.partitionSize = partitionSize;
	}
	
	public abstract int getPartition(T input);
	
	public int getPartitionSize() {
		return partitionSize;
	}
}
