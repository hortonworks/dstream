package org.apache.dstream.support;


public class DefaultHashPartitioner<T> implements Partitioner<T> {

	private static final long serialVersionUID = -3799649258371438298L;

	private final int partitionSize;
	
	public DefaultHashPartitioner(int partitionSize){
		this.partitionSize = partitionSize;
	}
	
	public int getPartition(T input) {
		return (input.hashCode() & Integer.MAX_VALUE) % this.partitionSize;
	}
	
	public int getPartitionSize() {
		return partitionSize;
	}

}
