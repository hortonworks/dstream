package org.apache.dstream.utils;

import java.util.Map.Entry;
/**
 * 
 *
 */
public class Utils {

	/**
	 * Utility method to create {@link Entry} from provided key/value pairs
	 * 
	 * @param key
	 * @param val
	 * @return
	 */
	public static <K,V> Entry<K,V> toEntry(final K key, final V val) {
		return new Entry<K, V>() {

			@Override
			public K getKey() {
				return key;
			}

			@Override
			public V getValue() {
				return val;
			}

			@Override
			public V setValue(V value) {
				throw new UnsupportedOperationException("This entry is immutable");
			}
		};
	}
}
