package org.apache.dstream;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * Strategy which exposes operations that are carriers of the <i>partitioning</i> instruction 
 * as well as the <i>aggregate-by-key</i> instruction to be applied during or after the actual partitioning. 
 * The point at which <i>aggregate-by-key</i> instruction is applied (during or after) is undefined since it depends on 
 * the implementation of shuffle/partition functionality of the target execution environment. 
 * 
 * @param <K> - 'key' of Key/Value pairs
 * @param <V> - 'value' of Key/Value pairs
 */
public interface Distributable<K,V> extends Partitionable<Entry<K,V>>, Combinable<K, V>, Joinable<K,V>, Groupable<K, V>, Serializable {
	
}