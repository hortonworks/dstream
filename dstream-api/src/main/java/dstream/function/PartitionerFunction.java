package dstream.function;

import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.utils.Assert;

/**
 * Base implementation of the  partitioner to be used by the target system.
 * 
 * @param <T> the type of the element that will be sent to a target partitioner
 * to determine partition id.
 */
public abstract class PartitionerFunction<T> implements SerFunction<T, Integer> {
	private static final long serialVersionUID = -250807397502312547L;
	
	private final int partitionSize;
	
	private SerFunction<? super T, ?> classifier;

	/**
	 * Constructs this function.
	 * 
	 * @param partitionSize the size of partitions
	 */
	public PartitionerFunction(int partitionSize) {
		Assert.isTrue(partitionSize > 0, "'partitionSize' must be > 0");
		this.partitionSize = partitionSize;
	}

	/**
	 * @return the size of partitions
	 */
	public int getPartitionSize(){
		return this.partitionSize;
	}
	
	/**
	 * Allows to set the classifier {@link SerFunction} function to extract value 
	 * used to determine partition id.
	 * 
	 * @param classifier function to extract value used by a target partitioner.
	 */
	public void setClassifier(SerFunction<? super T, ?> classifier) {
		this.classifier = classifier;
	}
	
	/**
	 * Returns <i>classifier</i> function used by the instance of this partitioner.
	 * Could be <i>null</i> if not set.
	 * 
	 * @return function to extract value used by a target partitioner.
	 */
	public SerFunction<? super T, ?> getClassifier() {
		return this.classifier;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return this.getClass().getSimpleName() + ":" + this.partitionSize;
	}
}
