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
package io.dstream.function;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import io.dstream.SerializableStreamAssets.SerBinaryOperator;

/**
 * An extension to {@link ValuesReducingFunction} which simply ensures the 
 * semantical correctness of the value (Iterable[V]). In a typical case where more then one
 * value existed in the first place it is the expectation that the <i>combiner</i> will 
 * produce an {@link Iterable}. However, for cases where there was only one value, <i>combiner</i> 
 * is never called, so this function ensures that such single value is wrapped into {@link Iterable}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ValuesAggregatingFunction<K,V,T> extends  ValuesReducingFunction<K, V, T> {
	private static final long serialVersionUID = 5774838692658472433L;
	
	/**
	 * Constructs this function.
	 * 
	 * @param aggregator an aggregate function, used to resolve collisions between
     *                      values associated with the same key.
	 */
	@SuppressWarnings("rawtypes")
	public ValuesAggregatingFunction(SerBinaryOperator aggregator) {
		super(aggregator);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Object buildValue(Stream<V> valuesStream){
		Object value = super.buildValue(valuesStream);
		if (!(value instanceof Iterable)){
			List<V> l = new ArrayList<V>();
			l.add((V) value);
			value = l;
		}
		return value;
	}
}
