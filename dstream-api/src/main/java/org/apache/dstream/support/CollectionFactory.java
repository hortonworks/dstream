package org.apache.dstream.support;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A general factory strategy to supply implementation of standard 
 * collections.
 *
 */
public interface CollectionFactory {

	/**
	 * Returns new instance of {@link List}
	 * 
	 * @return new instance of {@link List}
	 */
	<T> List<T> newList();
	
	/**
	 * Returns new instance of {@link Map}
	 * 
	 * @return new instance of {@link Map}
	 */
	<K,V> Map<K,V> newMap();
	
	/**
	 * Returns new instance of {@link Set}
	 * 
	 * @return new instance of {@link Set}
	 */
	<T> Set<T> newSet();
}
