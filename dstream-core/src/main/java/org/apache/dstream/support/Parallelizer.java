package org.apache.dstream.support;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;

public abstract class Parallelizer<T> implements Function<T, Integer> {
	private static final long serialVersionUID = -250807397502312547L;
	
	private int partitionSize;
	
	private final Function<? super T, ?> classifier;

	public Parallelizer(int partitionSize) {
		this(partitionSize, null);
	}
	
	public Parallelizer(int partitionSize, Function<? super T, ?> classifier) {
		Assert.isTrue(partitionSize > 0, "'partitionSize' must be > 0");
		this.partitionSize = partitionSize;
		this.classifier = classifier;
	}

	public int getPartitionSize(){
		return this.partitionSize;
	}
	
	public void updatePartitionSize(int partitionSize){
		Assert.isTrue(partitionSize > 0, "'partitionSize' must be > 0");
		this.partitionSize = partitionSize;
	}
	
	public Function<? super T, ?> getClassifier() {
		return classifier;
	}
}
