package org.apache.dstream.support;

import org.apache.dstream.Splitter;
import org.apache.dstream.support.SerializableFunctionConverters.Function;

/**
 * 
 *
 * @param <T>
 */
public class HashSplitter<T> extends Splitter<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	public HashSplitter(int splitSize){
		this(splitSize, null);
	}
	
	public HashSplitter(int splitSize, Function<? super T, ?> classifier){
		super(splitSize, classifier);
	}

	@Override
	public Integer apply(T input) {
		Object hashValue = input;
		if (this.getClassifier() != null){
			hashValue = this.getClassifier().apply(input);
		}
		return (hashValue.hashCode() & Integer.MAX_VALUE) % this.getSplitSize();
	}
}
