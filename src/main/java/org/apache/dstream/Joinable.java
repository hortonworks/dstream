package org.apache.dstream;

import org.apache.dstream.utils.Pair;
import org.apache.dstream.utils.SerializableBiFunction;

/**
 * 
 * Strategy which defines functionality to perform key-based joins between two {@link IntermediateResult}s.
 *
 * @param <K>
 * @param <V>
 */
public interface Joinable<K,V> {

	/**
	 * Will join two {@link IntermediateResult}s together based on the key.
	 * Values will be combined into a pair represented as {@link Pair}.
	 * 
	 * @param intermediateResult
	 * @return
	 */
	public <R> IntermediateResult<K,Pair<V,R>> join(IntermediateResult<K, R> intermediateResult);
	
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
