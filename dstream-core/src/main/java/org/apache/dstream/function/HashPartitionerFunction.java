package org.apache.dstream.function;

/**
 * 
 *
 * @param <T>
 */
public class HashPartitionerFunction<T> extends PartitionerFunction<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	public HashPartitionerFunction(int partitionSize){
		super(partitionSize);
	}

	@Override
	public Integer apply(T input) {
		Object hashValue = input;
		if (this.getClassifier() != null){
			hashValue = this.getClassifier().apply(input);
		}
		int ret = (hashValue.hashCode() & Integer.MAX_VALUE) % this.getPartitionSize();
		return ret;
	}
}
