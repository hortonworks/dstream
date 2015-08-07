/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.utils;

import java.io.Serializable;
import java.util.Map.Entry;
/**
 * Factory class to create {@link Serializable} version of {@link Entry}.
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
	
	/**
	 * {@link Serializable} implementation of {@link Entry}
	 * 
	 * @param <K> - key type
	 * @param <V> - value type
	 */
	static class SerializableEntry<K,V> implements Entry<K, V>, Serializable {
		private static final long serialVersionUID = 6756792191753240475L;

		private final K key;
		
		private final V value;
		
		/**
		 * 
		 * @param key
		 * @param value
		 */
		SerializableEntry(K key, V value){
			this.key = key;
			this.value = value;
		}
		
		/**
		 * 
		 */
		@Override
		public K getKey() {
			return this.key;
		}

		/**
		 * 
		 */
		@Override
		public V getValue() {
			return this.value;
		}

		/**
		 * 
		 */
		@Override
		public V setValue(V value) {
			throw new UnsupportedOperationException("This entry is immutable");
		}
		
		/**
		 * 
		 */
		@Override
		public int hashCode(){
			return this.key.hashCode() & this.value.hashCode();
		}
		
		/**
		 * 
		 */
		@Override
		public String toString(){
			return this.key + "=" + this.value;
		}
		
		/**
		 * 
		 */
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Entry){
				return ((Entry<?,?>)obj).getKey().equals(this.getKey()) && ((Entry<?,?>)obj).getValue().equals(this.getValue()); 
			} 
			return false;
	    }
	}
}
