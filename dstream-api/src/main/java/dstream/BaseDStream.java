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

import dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.function.SerializableFunctionConverters.SerPredicate;
import dstream.support.Classifier;
import dstream.support.HashClassifier;
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
	 * Returns an equivalent {@link DStream} where elements are classified
	 * on values provided by the given <i>classifier</i>.<br>
	 * Classification is performed via implementation of {@link Classifier}.
	 * Default implementation of the {@link Classifier} is {@link HashClassifier},
	 * however specialized implementation could be configured via {@link DStreamConstants#CLASSIFIER}
	 * configuration property.<br>
	 * For example:
	 * <pre>
	 * // Suppose you have a text file with the following contents:
	 * foo bar foo bar foo
	 * bar foo bar foo
	 * 
	 * // Your pipeline
	 * DStream.ofType(String.class, "wc")
	 *     .flatMap(record -> Stream.of(record.split("\\s+")))
	 *     .group(word -> word)
	 * 
	 * // Your GroupingFunction is configured as 'dstream.grouper=FooBarGrouper' 
	 * // Its implementation looks like this:
	 * 
	 * class FooBarGrouper extends GroupingFunction {
	 *     public Integer apply(Object input) {
	 *         return input.equals("bar") ? 1 : 0;
	 *     }
	 * }
	 * </pre>
	 * The above would result in {@link DStream} which represent two groups:<br>
	 * Group-1 - bar bar bar bar<br>
	 * Group-2 - foo foo foo foo foo<br>
	 * Even though it would look continuous to you 
	 * (i.e., bar bar bar bar foo foo foo foo foo).<br>
	 * <br>
	 * In the "distributable" reality, this often implies data <i>partitioning</i>, since 
	 * {@link Classifier} is compliant with the general semantics of partitioners
	 * by returning an {@link Integer} representing the partition ID.<br>
	 * <br>
	 * However, the actual <b><i>data partitioning</i></b> is the function of the system and 
	 * exist primarily to facilitate greater parallelisation when it comes to actual data processing. 
	 * <b><i>Data grouping</i></b> on the other hand, is the function of the application deriving its 
	 * requirement from the use case at hand (e.g., group all 'foo's and 'bar's together).<br>
	 * So, it is important to separate the two, since it is quite conceivable that to facilitate greater 
	 * parallelisation in the truly distributed environment the groups
	 * could be further partitioned (e.g., 2 groups into 8 partitions).<br>
	 * <br>
	 * Another configuration property relevant to this and every other <i>shuffle</i>-style operation
	 * is {@link DStreamConstants#PARALLELISM} which allows you to provide a hint as to the level of
	 * parallelisation you may want to accomplish and is typically sent as one of the constructor arguments
	 * to the instance of {@link Classifier}, but it could also be used by the target execution 
	 * environment to configure its partitioner if the two concerns are different (i.e., partition
	 * however many groups into the amount of partitions specified by the {@link DStreamConstants#PARALLELISM}
	 * configuration property).<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract value used by a target partitioner.
	 * @return
	 */
	T classify(SerFunction<? super A, ?> classifier);
	
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
	 * elements of this stream are grouped on the given <i>groupClassifier</i> (e.g., key) and 
	 * reduced by the given <i>valueReducer</i>.<br>
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
	<K,V> DStream<Entry<K,V>> reduceValues(SerFunction<? super A, ? extends K> groupClassifier, 
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
	<K,V> DStream<Entry<K,List<V>>> aggregateValues(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper);
	
}
