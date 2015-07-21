package org.apache.dstream.utils;

import java.io.Serializable;
import java.util.Map.Entry;
/**
 * 
 */
public class KVUtils {

	/**
	 * Utility method to create {@link Entry} from provided key/value pairs
	 * 
	 * @param key
	 * @param val
	 * @return
	 * 
	 * @param <K> key type
	 * @param <V> value type
	 */
	public static <K,V> Entry<K,V> kv(final K key, final V val) {
		return new SerializableEntry<>(key, val);
	}
	
	static class SerializableEntry<K,V> implements Entry<K, V>, Serializable {
		private static final long serialVersionUID = 6756792191753240475L;

		private final K key;
		
		private final V value;
		
		SerializableEntry(K key, V value){
			this.key = key;
			this.value = value;
		}
		@Override
		public K getKey() {
			return this.key;
		}

		@Override
		public V getValue() {
			return this.value;
		}

		@Override
		public V setValue(V value) {
			throw new UnsupportedOperationException("This entry is immutable");
		}
		
		@Override
		public String toString(){
			return this.key + "=" + this.value;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Entry){
				return ((Entry<?,?>)obj).getKey().equals(this.getKey()) && ((Entry<?,?>)obj).getValue().equals(this.getValue()); 
			} 
			else {
				return false;
			}
	    }
	}
}
