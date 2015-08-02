package org.apache.dstream.function;

import org.apache.dstream.function.SerializableFunctionConverters.Function;


/**
 * 
 *
 * @param <T>
 */
public class HashPartitionerFunction<T> extends PartitionerFunction<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	public HashPartitionerFunction(int splitSize){
		this(splitSize, null);
	}
	
	public HashPartitionerFunction(int splitSize, Function<? super T, ?> classifier){
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
