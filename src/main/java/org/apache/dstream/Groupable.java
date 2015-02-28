package org.apache.dstream;

import org.apache.dstream.utils.SerializableBiFunction;

public interface Groupable<K,V> {

	public IntermediateResult<K,V> groupByKey();
	
	/**
	 * Will group values based on the key.
	 * Values will be combined into an object of type R by 'valueCombiner'.
	 * 
	 * @param intermediateResult
	 * @param valueCombiner 
	 * @return
	 */
	public <R> IntermediateResult<K,R> groupByKey(SerializableBiFunction<V, V, R> valueCombiner);
}
