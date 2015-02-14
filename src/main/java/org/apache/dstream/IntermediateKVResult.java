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
public interface IntermediateKVResult<K,V> {
	/**
	 * Will perform a post-shuffle reduce by key, producing the same Key/Value types as declared by 
	 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
	 * Similar to the 'compute*' methods of {@link StreamExecutionContext} and {@link IntermediateStageEntryPoint} 
	 * this method signifies starting point for a new Stage/Vertex in a DAG-like implementation.
	 * 
	 * @param mergeFunction
	 * @param reducers
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> reduceByKey(BinaryOperator<V> mergeFunction, int reducers);
		
	/**
	 * Will perform a post-shuffle reduce by value, producing the same Key/Value types as declared by 
	 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
	 * Similar to the 'compute*' methods of {@link StreamExecutionContext} and {@link IntermediateStageEntryPoint} 
	 * this method signifies starting point for a new Stage/Vertex in a DAG-like implementation.
	 * 
	 * @param mergeFunction
	 * @param reducers
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> reduceByValue(BinaryOperator<K> mergeFunction, int reducers);
	
	/**
	 * Will perform a post-shuffle reduce passing the whole {@link Entry}, producing the same Key/Value types as declared by 
	 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
	 * Similar to the 'compute*' methods of {@link StreamExecutionContext} and {@link IntermediateStageEntryPoint} 
	 * this method signifies starting point for a new Stage/Vertex in a DAG-like implementation.
	 * 
	 * See {@link #reduceByKey(BinaryOperator, int)} and {@link #reduceByValue(BinaryOperator, int)} as well
	 * 
	 * @param mergeFunction
	 * @param reducers
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> reduce(BinaryOperator<Entry<K,V>> mergeFunction, int reducers);
	
	/**
	 * Will partition the intermediate result using provided {@link Partitioner}
	 * 
	 * @param partitioner
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> partition(Partitioner partitioner);
	
	/**
	 * ill partition the intermediate result using provided partitioning function.
	 * 
	 * @param partitionerFunction
	 * @return
	 */
	public IntermediateStageEntryPoint<Entry<K,V>> partition(SerializableFunction<Entry<K,V>, Integer> partitionerFunction);
}