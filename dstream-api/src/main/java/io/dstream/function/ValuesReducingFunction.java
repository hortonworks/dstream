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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.dstream.SerializableStreamAssets.SerBinaryOperator;
import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.utils.KVUtils;

/**
 * Will combine (reduce) values of a {@link Stream} who's elements are Key/Values pairs 
 * as in [K, Iterator[V]] using provided 'combiner' producing a new {@link Stream}
 * with [K,V] semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ValuesReducingFunction<K,V,T> implements SerFunction<Stream<Entry<K,Iterator<V>>>,Stream<T>> {
	private static final long serialVersionUID = 1133920289646508908L;
	
	@SuppressWarnings("rawtypes")
	private final SerBinaryOperator reducer;
	
	/**
	 * Constructs this function.
	 * 
	 * @param reducer a reduce function, used to resolve collisions between
     *                      values associated with the same key.
	 */
	@SuppressWarnings("rawtypes")
	public ValuesReducingFunction(SerBinaryOperator reducer) {
		this.reducer = reducer;
	}

	/**
	 * 
	 */
	@Override
	public Stream<T> apply(Stream<Entry<K, Iterator<V>>> sourceStream) {
		return sourceStream.map(entry -> this.mergeValuesForCurrentKey(entry));
	}
	
	/**
	 * 
	 * @param valuesStream
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Object buildValue(Stream<V> valuesStream){
		return valuesStream.reduce(this.reducer).get();
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private T mergeValuesForCurrentKey(Entry<K, Iterator<V>> currentEntry){
		Stream<V> valuesStream = (Stream<V>) StreamSupport.stream(Spliterators.spliteratorUnknownSize(currentEntry.getValue(), Spliterator.ORDERED), false);
		Object value = this.reducer == null ? valuesStream.findFirst().get() : KVUtils.kv(currentEntry.getKey(), this.buildValue(valuesStream));
		return (T) value;
	}
}
