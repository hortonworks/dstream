package org.apache.dstream.function;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;

public abstract class PartitionerFunction<T> implements Function<T, Integer> {
	private static final long serialVersionUID = -250807397502312547L;
	
	private int partitionSize;
	
	private Function<? super T, ?> classifier;

	public PartitionerFunction(int partitionSize) {
		Assert.isTrue(partitionSize > 0, "'partitionSize' must be > 0");
		this.partitionSize = partitionSize;
	}

	public int getPartitionSize(){
		return this.partitionSize;
	}
	
	public void updatePartitionSize(int partitionSize){
		Assert.isTrue(partitionSize > 0, "'partitionSize' must be > 0");
		this.partitionSize = partitionSize;
	}
	
	public void setClassifier(Function<? super T, ?> classifier) {
		this.classifier = classifier;
	}
	
	public Function<? super T, ?> getClassifier() {
		return this.classifier;
	}
	
	@Override
	public String toString(){
		return this.getClass().getSimpleName() + ":" + partitionSize;
	}
}
