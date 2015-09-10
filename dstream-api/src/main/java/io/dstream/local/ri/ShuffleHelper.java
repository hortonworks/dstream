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
package io.dstream.local.ri;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.dstream.support.Aggregators;
/**
 * 
 *
 */
class ShuffleHelper {
	
	/**
	 * 
	 */
	static class RefHolder {
		public final Object ref;
		public RefHolder(Object ref){
			this.ref = ref;
		}
		@Override
		public String toString(){
			return this.ref.toString();
		}
		
		@Override
		public int hashCode(){
			return this.ref.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return obj instanceof RefHolder && this.ref.equals(((RefHolder)obj).ref);
		}
	}

	/**
	 * 
	 * @param v1
	 * @param v2
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T> T group(Object v1, Object v2) {
		RefHolder refHolder = (RefHolder) v2;
		v2 = refHolder.ref;
		
		T aggregatedValues;
		if (v2 instanceof Entry){
			aggregatedValues = (T) toMap(v1);
			Entry<Object, Object> entry = (Entry<Object, Object>) v2;
			((Map<Object, Object>)aggregatedValues).merge(entry.getKey(), entry.getValue(), Aggregators::aggregateToList);
		}
		else {
			aggregatedValues = (T)toList(v1);
			((List)aggregatedValues).add(v2);
		}
		
		return aggregatedValues;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static <K,V> Map<K,V> toMap(Object value) {
		if (value instanceof RefHolder){
			value = ((RefHolder)value).ref;
		}
		Map<K,V> aggregatedValues;
		if (value instanceof Map){
			aggregatedValues = (Map<K,V>) value;
		}
		else {
			aggregatedValues = new HashMap<>();
			Entry<K,V> entry = (Entry<K,V>) value;
			aggregatedValues.put(entry.getKey(), entry.getValue());
		}
		return aggregatedValues;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private static <T> List<T> toList(Object value){
		if (value instanceof RefHolder){
			value = ((RefHolder)value).ref;
		}
		List<T> aggregatedValues;
		if (value instanceof List){
			aggregatedValues = (List<T>)value;
		}
		else {
			aggregatedValues = new ArrayList<T>();
			aggregatedValues.add((T) value);
		}
		return aggregatedValues;
	}
}
