package org.apache.dstream;


public interface Joinable<K,V> {

	public IntermediateResult<K,V> join(IntermediateResult<K, V> intermediateResult);
}
