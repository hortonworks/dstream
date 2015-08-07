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
package dstream;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import dstream.function.PartitionerFunction;
import dstream.function.SerializableFunctionConverters.SerBiFunction;
import dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.function.SerializableFunctionConverters.SerPredicate;
/**
 * Base strategy for {@link DStream} which contains all common operations.
 * 
 * @param <A> the type of the stream elements
 * @param <T> the actual type of the instance of this {@link BaseDStream}.
 */
interface BaseDStream<A, T> extends ExecutableDStream<A> {

	/**
	 * Will <b>distinctively</b> combine two streams of the same type returning a new {@link DStream} 
	 * of the same type.
	 * <pre>
	 * DStream&lt;String&gt; d1 = ...
	 * DStream&lt;String&gt; d2 = ...
	 * DStream&lt;String&gt; d3 = ...
	 * 
	 * d1.union(d2) - legal
	 * d1.union(d3) - is illegal and will result in compile error.
	 * </pre>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-shuffle</i> operation.
	 * 
	 * @param stream {@link DStream} of the same type to combine with this stream.
	 * @return new {@link DStream} of the same type.
	 */
	T union(T stream);
	
	/**
	 * Will combine two streams of the same type returning a new {@link DStream} 
	 * of the same type.
	 * <pre>
	 * DStream&lt;String&gt; d1 = ...
	 * DStream&lt;String&gt; d2 = ...
	 * DStream&lt;String&gt; d3 = ...
	 * 
	 * d1.union(d2) - legal
	 * d1.union(d3) - is illegal and will result in compile error.
	 * </pre>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-shuffle</i> operation.
	 * 
	 * @param stream {@link DStream} of the same type to combine with this stream.
	 * @return new {@link DStream} of the same type.
	 */
	T unionAll(T stream);
	
	/**
	 * Returns a {@link DStream} consisting of the elements of this stream that match
     * the given predicate.<br>
     * <br>
     * Consistent with {@link Stream#filter(java.util.function.Predicate)}.<br>
     * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 * 
	 * @param predicate predicate to apply to each element to determine if it
     *                  should be included
	 * @return new {@link DStream}
	 */
	T filter(SerPredicate<? super A> predicate);
	
	/**
	 * Returns an equivalent {@link DStream} that is partitioned. 
	 * Typically used as a signal to a target execution environment to 
	 * partition the stream using the entire value of each element of the stream.<br>
	 * <br>
	 * Partition size is configurable externally via {@link DStreamConstants#PARALLELISM}
	 * property.<br>
	 * Custom partitioner can also be provided as an instance of {@link PartitionerFunction}
	 * via {@link DStreamConstants#PARTITIONER} property.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @return new {@link DStream}
	 */
	T partition();
	
	/**
	 * Returns an equivalent {@link DStream} that is partitioned. 
	 * Typically used as a signal to a target execution environment to 
	 * partition the stream using the value extracted by <i>classifier</i>
	 * function.<br>
	 * <br>
	 * Partition size is configurable externally via {@link DStreamConstants#PARALLELISM}
	 * property.<br>
	 * Custom partitioner can also be provided as an instance of {@link PartitionerFunction}
	 * via {@link DStreamConstants#PARTITIONER} property.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract value used by a target partitioner.
	 * @return
	 */
	T partition(SerFunction<? super A, ?> classifier);
	
	/**
	 * Returns a {@link DStream} consisting of the results of replacing each element of
     * this stream with the contents of a mapped stream produced by applying
     * the provided mapping function to each element.<br>
     * <br>
     * Consistent with {@link Stream#flatMap(java.util.function.Function)}.<br>
     * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 * 
     * @param <R> The element type of the returned {@link DStream}
	 * @param mapper function to apply to each element which produces a stream
     *               of new values
	 * @return new {@link DStream}
	 */
	<R> DStream<R> flatMap(SerFunction<? super A, ? extends Stream<? extends R>> mapper);
	
	/**
	 * Returns a {@link DStream} consisting of the results of applying the given
     * function to the elements of this stream.<br>
     * <br>
     * Consistent with {@link Stream#map(java.util.function.Function)}.<br>
     * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 * 
     * @param <R> The element type of the returned {@link DStream}
	 * @param mapper function to apply to each element
	 * @return new {@link DStream}
	 */
	<R> DStream<R> map(SerFunction<? super A, ? extends R> mapper);
	
	/**
	 * Returns a {@link DStream} consisting of the results of applying the given function
	 * on the entire stream (i.e., partition/split) represented as {@link Stream}.<br>
	 * This operation essentially allows to fall back on standard {@link Stream} API.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 * 
	 * @param <R> The element type of the returned {@link DStream}
	 * @param computeFunction function to apply on the entire {@link Stream}.
	 * @return new {@link DStream}
	 */
	<R> DStream<R> compute(SerFunction<? super Stream<A>, ? extends Stream<? extends R>> computeFunction);
	
	/**
	 * Returns a {@link DStream} of Key/Value pairs, where values mapped from the individual 
	 * elements of this stream are grouped on the given <i>groupClassifier</i> (e.g., key) and reduced by the 
	 * given <i>valueReducer</i>.<br>
	 * <br> 
	 * This operation is similar to <i>Stream.collect(Collectors.toMap(Function, Function, BinaryOperator))</i>, 
	 * yet it is not terminal.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param <K> The element type of the key
	 * @param <V> The element type of the value
	 * @param groupClassifier a mapping function to produce keys
	 * @param valueMapper a mapping function to produce values
	 * @param valueReducer a reduce function, used to resolve collisions between
     *                      values associated with the same key.
	 * @return new {@link DStream} of Key/Value pairs represented as {@link Entry}&lt;K,V&gt;
	 */
	<K,V> DStream<Entry<K,V>> reduceGroups(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper,
			SerBinaryOperator<V> valueReducer);
	
	/**
	 * Returns a {@link DStream} of Key/Value pairs, where values mapped from the individual 
	 * elements of this stream are grouped on the given <i>groupClassifier</i> (e.g., key) and
	 * aggregated into a {@link List}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param <K> The element type of the key
	 * @param <V> The element type of the value
	 * @param groupClassifier a mapping function to produce keys
	 * @param valueMapper a mapping function to produce values
	 * @return new {@link DStream} of Key/Value pairs represented as {@link Entry}&lt;K,List&lt;V&gt;&gt; 
	 */
	<K,V> DStream<Entry<K,List<V>>> aggregateGroups(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper);
	
	/**
	 * Returns a {@link DStream} of Key/Value pairs, where values mapped from the individual 
	 * elements of this stream are grouped on the given <i>groupClassifier</i> (e.g., key) and 
	 * aggregated into value of type F by the given <i>valueAggregator</i>.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param <K> The element type of the key
	 * @param <V> The element type of the value
	 * @param <F> The element type of the aggregated values.
	 * @param groupClassifier a mapping function to produce keys
	 * @param valueMapper a mapping function to produce values
	 * @param valueAggregator an aggregate function, used to resolve collisions between
     *                      values associated with the same key.
	 * @return new {@link DStream} of Key/Value pairs represented as {@link Entry}&lt;K,F&gt; 
	 */
	<K,V,F> DStream<Entry<K,F>> aggregateGroups(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper,
			SerBiFunction<?,V,F> valueAggregator);
	
}
