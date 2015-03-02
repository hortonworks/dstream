package org.apache.dstream;

/**
 * 
 * Strategy which defines functionality to perform key-based grouping of values
 *
 * @param <K>
 * @param <V>
 */
public interface Groupable<K,V> {

	/**
	 * Will group values based on the key.
	 * Values will be combined into {@link Iterable} with elements of type V.
	 * 
	 * @return
	 */
	public IntermediateResult<K,Iterable<V>> groupByKey();
}
