package org.apache.dstream;

import java.util.Map.Entry;

import org.apache.dstream.utils.SerializableBiFunction;

/**
 * 
 * Strategy which defines functionality to perform joins between to {@link IntermediateResult}s 
 * based on the key.
 *
 * @param <K>
 * @param <V>
 */
public interface Joinable<K,V> {

	/**
	 * Will join two {@link IntermediateResult}s together based on the key.
	 * Values will be combined into a pair represented as {@link Entry}.[QUESTION] - use of Entry is still discussed
	 * 
	 * @param intermediateResult
	 * @return
	 */
	public IntermediateResult<K,V> join(IntermediateResult<K, V> intermediateResult);
	
	/**
	 * Will join two {@link IntermediateResult}s together based on the key.
	 * Values will be combined into an object of type R by 'valueCombiner'.
	 * 
	 * @param intermediateResult
	 * @param valueCombiner 
	 * @return
	 */
	public <KK,VV,R> IntermediateResult<K,R> join(IntermediateResult<KK, VV> intermediateResult, SerializableBiFunction<V, VV, R> valueCombiner);
}
