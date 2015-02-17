package org.apache.dstream;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableFunction;

/**
 * Strategy which represents intermediate KEY/VALUE results as {@link Entry}. It is returned by 
 * {@link StreamExecutionContext#computeAsKeyValue(Class, Class, SerializableFunction)} method.
 * <br>
 * Intermediate results are the post-shuffle (read from the shuffled input) 
 * 
 * @param <K>
 * @param <V>
 */
public interface IntermediateKVResult<K,V> extends Submittable<Entry<K,V>> {
	/**
	 * Will partition the intermediate result using default {@link Partitioner} provided by the underlying execution environment.
	 * When partitions are written the 'mergeFunction' will also be applied.
	 * 
	 * @param partitioner
	 * @return
	 */
	public Submittable<Entry<K,V>> partition(int partitionSize, BinaryOperator<V> mergeFunction);
	/**
	 * Will partition the intermediate result using provided {@link Partitioner}
	 * When partitions are written the 'mergeFunction' will also be applied.
	 * 
	 * @param partitioner
	 * @return
	 */
	public Submittable<Entry<K,V>> partition(Partitioner partitioner, BinaryOperator<V> mergeFunction);
	
	/**
	 * Will partition the intermediate result using provided partitioning function. It is assumed that partitioning function 
	 * maintains knows about the maximum number of partitions. 
	 * When partitions are written the 'mergeFunction' will also be applied.
	 * 
	 * @param partitionerFunction
	 * @return
	 */
	public Submittable<Entry<K,V>> partition(SerializableFunction<Entry<K,V>, Integer> partitionerFunction, BinaryOperator<V> mergeFunction);
	
}