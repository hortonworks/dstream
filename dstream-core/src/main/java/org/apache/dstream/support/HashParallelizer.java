package org.apache.dstream.support;

import org.apache.dstream.support.SerializableFunctionConverters.Function;

/**
 * 
 *
 * @param <T>
 */
public class HashParallelizer<T> extends Parallelizer<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	public HashParallelizer(int partitionSize){
		this(partitionSize, null);
	}
	
	public HashParallelizer(int partitionSize, Function<? super T, ?> classifier){
		super(partitionSize, classifier);
	}

	@Override
	public Integer apply(T input) {
		Object hashValue = input;
		if (this.getClassifier() != null){
			hashValue = this.getClassifier().apply(input);
		}
		return (hashValue.hashCode() & Integer.MAX_VALUE) % this.getPartitionSize();
	}
}
