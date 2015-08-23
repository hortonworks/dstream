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
package dstream.function;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.SerializableStreamAssets.SerFunction;
import dstream.utils.Assert;
import dstream.utils.KVUtils;

/**
 * Implementation of {@link SerFunction} to produce {@link Stream} of Key/Value pairs 
 * from another {@link Stream}.<br>
 * Key/Value pairs represented as {@link Entry} 
 * <br>
 * Key/Values are created using <i>keyExtractor</i> and <i>valueExtractor</i> provided 
 * during the construction.
 *
 * @param <T> the type of the source stream
 * @param <K> the key type
 * @param <V> the value type
 */
public class KeyValueMappingFunction<T,K,V> implements SerFunction<Stream<T>, Stream<Entry<K, V>>> {
	private static final long serialVersionUID = -4257572937412682381L;
	
	private final SerFunction<T, K> keyExtractor;
	
	private final SerFunction<T, V> valueExtractor;
	
	private final BinaryOperator<V> aggregator;
	
	/**
	 * Constructs this function.
	 * 
	 * @param keyExtractor a mapping function to produce keys
	 * @param valueExtractor a mapping function to produce values
	 */
	public KeyValueMappingFunction(SerFunction<T, K> keyExtractor, SerFunction<T, V> valueExtractor) {
		this(keyExtractor, valueExtractor, null);
	}
	
	/**
	 * Constructs this function.
	 * 
	 * @param keyExtractor a mapping function to produce keys
	 * @param valueExtractor a mapping function to produce values
	 * @param combiner a combine function, used to resolve collisions between
     *                      values associated with the same key.
	 */
	public KeyValueMappingFunction(SerFunction<T, K> keyExtractor, SerFunction<T, V> valueExtractor, BinaryOperator<V> aggregator) {
		Assert.notNull(keyExtractor, "'keyExtractor' must not be null");
		Assert.notNull(valueExtractor, "'valueExtractor' must not be null");
		
		this.keyExtractor = keyExtractor;
		this.valueExtractor = valueExtractor;
		this.aggregator = aggregator;
	}

	/**
	 * Will create a new {@link Stream} of Key/Value pairs represented as {@link Entry} 
	 */
	@Override
	public Stream<Entry<K, V>> apply(Stream<T> streamIn) {	
		Assert.notNull(streamIn, "'streamIn' must not be null");
		if (this.aggregator != null){
			return streamIn.collect(Collectors.toMap(this.keyExtractor, this.valueExtractor, this.aggregator)).entrySet().stream();
		}
		else {
			return streamIn.map(val -> KVUtils.kv(this.keyExtractor.apply(val), this.valueExtractor.apply(val)));
		}
	}
	
	/**
	 * Returns true is this function will aggregate values
	 * @return
	 */
	public boolean aggregatesValues(){
		return this.aggregator != null;
	}
}
