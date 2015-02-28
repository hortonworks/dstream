package org.apache.dstream;

import java.io.Serializable;
import java.util.Map.Entry;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableBinaryOperator;
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
public interface IntermediateResult<K,V> extends Partitionable<Entry<K,V>>, Serializable {
	
	/**
	 * Will partition the intermediate result using default {@link Partitioner} provided by the underlying execution environment.
	 * When partitions are written the 'mergeFunction' will also be applied.
	 * 
	 * @param partitioner
	 * @return
	 */
	public Submittable<Entry<K,V>> merge(int partitionSize, SerializableBinaryOperator<V> mergeFunction);
	/**
	 * Will partition the intermediate result using provided {@link Partitioner}
	 * When partitions are written the 'mergeFunction' will also be applied.
	 * 
	 * @param partitioner
	 * @return
	 */
	public Submittable<Entry<K,V>> merge(Partitioner<Entry<K,V>> partitioner, SerializableBinaryOperator<V> mergeFunction);
	
	/**
	 * Will partition the intermediate result using provided partitioning function. It is assumed that partitioning function 
	 * maintains knows about the maximum number of partitions. 
	 * When partitions are written the 'mergeFunction' will also be applied.
	 * 
	 * @param partitionerFunction
	 * @return
	 */
	public Submittable<Entry<K,V>> merge(SerializableFunction<Entry<K,V>, Integer> partitionerFunction, SerializableBinaryOperator<V> mergeFunction);
	
}