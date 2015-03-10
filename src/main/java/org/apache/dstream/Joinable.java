package org.apache.dstream;

import org.apache.dstream.utils.Pair;
import org.apache.dstream.utils.SerializableBiFunction;

/**
 * 
 * Strategy which defines functionality to perform key-based joins between two {@link Distributable}s.
 *
 * @param <K>
 * @param <V>
 */
public interface Joinable<K,V> {

	/**
	 * Will join two {@link Distributable}s together based on the key.
	 * Values will be combined into a pair represented as {@link Pair}.
	 * 
	 * @param intermediateResult
	 * @return
	 */
	public <R> Distributable<K,Pair<V,R>> join(Distributable<K, R> intermediateResult);
	
	/**
	 * Will join two {@link Distributable}s together based on the key.
	 * Values will be combined into an object of type R by 'valueCombiner'.
	 * 
	 * @param intermediateResult
	 * @param valueCombiner 
	 * @return
	 */
	public <W,R> Distributable<K,R> join(Distributable<K, W> intermediateResult, SerializableBiFunction<V, W, R> valueCombiner);
}
