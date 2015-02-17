package org.apache.dstream;

import java.util.Map.Entry;

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
public interface IntermediateKVResult<K,V> extends IntermediateStageEntryPoint<Entry<K,V>>{
	/**
	 * Will partition the intermediate result using provided {@link Partitioner}
	 * 
	 * @param partitioner
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> partition(int partitionSize);
	/**
	 * Will partition the intermediate result using provided {@link Partitioner}
	 * 
	 * @param partitioner
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> partition(Partitioner partitioner, int partitionSize);
	
	/**
	 * ill partition the intermediate result using provided partitioning function.
	 * 
	 * @param partitionerFunction
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> partition(SerializableFunction<Entry<K,V>, Integer> partitionerFunction, int partitionSize);
}