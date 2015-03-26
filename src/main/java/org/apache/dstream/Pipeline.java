package org.apache.dstream;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.SerializableLambdas.BinaryOperator;
import org.apache.dstream.SerializableLambdas.Function;

public interface Pipeline<T> extends Submittable<T>, AutoCloseable, Serializable {

	<K,V> CombinablePipeline<K, V> computeMappings(Function<? extends Stream<T>, ? extends Stream<Entry<K,V>>> computeFunction);
	
	/**
	 * A companion method to support non Stream-based programming semantics for Key/Value output.
	 * 
	 * @param processor
	 * @return
	 */
	<K,V> CombinablePipeline<K, V> computeMappings(KeyValueProcessor<T,K,V> processor);
	
	
	<K,V> CombinablePipeline<K, V> computeMappings(Function<? extends Stream<T>, ? extends Stream<Entry<K,V>>> computeFunction,
			BinaryOperator<V> outputCombineFunction);
	
	/**
	 * Will compute values of type R. Unlike in compute mapping where return type will be {@link Entry} which can then be interpreted 
	 * as Key/Value pair by the executing processor, this value will be treated as single value. 
	 *  
	 * @param computeFunction
	 * @return
	 */
	<R> Pipeline<R> computeValues(Function<? extends Stream<T>, ? extends Stream<R>> computeFunction);
	
	/**
	 * A companion method to support non Stream-based programming semantics for non Key/Value output.
	 * 
	 * @param processor
	 * @return
	 */
	<R> Pipeline<R> computeValues(ValuesProcessor<T,R> processor);
	
	<R> Number count(Function<? extends Stream<T>, ? extends Number> computeFunction);
	
	<R> Number count(ItemCounter<T> processor);
	
	/**
	 * 
	 * @return
	 */
	//TODO Add another partition methods where partitioning logic may be provided for cases where 
	//     partitioning is a functional requirement
	Pipeline<T> partition();
	
}
