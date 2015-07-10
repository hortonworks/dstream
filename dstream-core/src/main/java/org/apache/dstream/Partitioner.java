package org.apache.dstream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;

public abstract class Partitioner<T> implements Function<T, Integer> {
	private static final long serialVersionUID = -250807397502312547L;
	
	private int splitSize;
	
	private final Function<? super T, ?> classifier;

	public Partitioner(int splitSize) {
		this(splitSize, null);
	}
	
	public Partitioner(int splitSize, Function<? super T, ?> classifier) {
		Assert.isTrue(splitSize > 0, "'splitSize' must be > 0");
		this.splitSize = splitSize;
		this.classifier = classifier;
	}

	public int getPartitionSize(){
		return this.splitSize;
	}
	
	public void updatePartitionSize(int splitSize){
		Assert.isTrue(splitSize > 0, "'splitSize' must be > 0");
		this.splitSize = splitSize;
	}
	
	public Function<? super T, ?> getClassifier() {
		return this.classifier;
	}
}
