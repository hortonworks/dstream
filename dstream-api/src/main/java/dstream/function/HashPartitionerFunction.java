package dstream.function;

/**
 * Implementation of the {@link PartitionerFunction} for hash based partitioning.
 *
 * @param <T> the type of the element that will be sent to a target partitioner
 * to determine partition id.
 */
public class HashPartitionerFunction<T> extends PartitionerFunction<T> {
	private static final long serialVersionUID = -3799649258371438298L;
	
	/**
	 * Constructs this function.
	 * 
	 * @param partitionSize the size of partitions
	 */
	public HashPartitionerFunction(int partitionSize){
		super(partitionSize);
	}

	/**
	 * 
	 */
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
