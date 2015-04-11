package org.apache.dstream.utils;

import java.util.List;
import java.util.Map.Entry;
/**
 * 
 *
 */
public class Utils {
	
	public static Entry<Object,List<Object>> mergeEntries(Entry<Object,List<Object>> t, Entry<Object,List<Object>> u) {
		t.getValue().addAll(u.getValue());
		return t;
	}

	/**
	 * Utility method to create {@link Entry} from provided key/value pairs
	 * 
	 * @param key
	 * @param val
	 * @return
	 */
	public static <K,V> Entry<K,V> kv(final K key, final V val) {
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
			
			@Override
			public String toString(){
				return "(" + key + " -> " + val + ")";
			}
			
			@Override
			public boolean equals(Object obj) {
				if (obj instanceof Entry){
					return ((Entry<?,?>)obj).getKey().equals(this.getKey()) && ((Entry<?,?>)obj).getValue().equals(this.getValue()); 
				} else {
					return false;
				}
		    }
		};
	}
}
