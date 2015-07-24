package org.apache.dstream.support;

import org.apache.dstream.function.SerializableFunctionConverters.Function;

/**
 * 
 *
 * @param <T>
 */
public class HashPartitioner<T> extends Partitioner<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	public HashPartitioner(int splitSize){
		this(splitSize, null);
	}
	
	public HashPartitioner(int splitSize, Function<? super T, ?> classifier){
		super(splitSize, classifier);
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
