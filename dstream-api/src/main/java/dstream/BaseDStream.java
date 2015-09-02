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

import dstream.SerializableStreamAssets.SerBinaryOperator;
import dstream.SerializableStreamAssets.SerComparator;
import dstream.SerializableStreamAssets.SerFunction;
import dstream.SerializableStreamAssets.SerPredicate;
import dstream.support.Classifier;
import dstream.support.HashClassifier;
/**
 * Base strategy for all variants of {@link DStream} and defines all common operations.
 *
 * @param <A> the type of the stream elements
 * @param <T> the actual type of the instance of this {@link BaseDStream}.
 */
interface BaseDStream<A, T> extends ExecutableDStream<A> {

	/**
	 * Returns a stream consisting of the distinct elements (according to
	 * {@link Object#equals(Object)}) of this stream.
	 * <br>
	 * Consistent with {@link Stream#distinct()}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 *
	 * @return new {@link DStream} of the same type.
	 */
	T distinct();

	/**
	 * Returns a stream containing a single value representing the count of elements in
	 * the previous stream.<br>
	 *
	 * This operation is a non-terminal equivalent of the
	 * <i>Stream.count()</i>.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 *
	 * @return new {@link DStream} of type {@link Long}
	 */
	DStream<Long> count();

	/**
	 * Will <b>distinctively</b> combine the two streams of the <i><b>same type</b></i>
	 * returning a new {@link DStream} of the same type. <br>
	 * For example:
	 * <pre>
	 * DStream&lt;String&gt; d1 = ...
	 * DStream&lt;String&gt; d2 = ...
	 * DStream&lt;Integer&gt; d3 = ...
	 *
	 * d1.union(d2) - legal because both streams are of the same type
	 * d1.union(d3) - is illegal and will result in compile error since two streams are of different type (String and Integer)
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
	 * DStream&lt;Integer&gt; d3 = ...
	 *
	 * d1.unionAll(d2) - legal because both streams are of the same type
	 * d1.unionAll(d3) - is illegal and will result in compile error since two streams are of different type (String and Integer)
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
	 * Returns a new {@link DStream} consisting of the elements of this stream that were tested
	 * true according to the given predicate.
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
	 * @return new {@link DStream} of the same type.
	 */
	T filter(SerPredicate<? super A> predicate);

	/**
	 * Returns an equivalent {@link DStream} where elements are classified
	 * on values provided by the given <i>classifier</i>.<br>
	 * Classification is performed by the {@link Classifier}.
	 * Default implementation of the {@link Classifier} is {@link HashClassifier},
	 * however specialized implementation could be configured via {@link DStreamConstants#CLASSIFIER}
	 * configuration property.<br>
	 * Classification could be looked at as the process of distributed grouping<br>
	 * For example:
	 * <pre>
	 * // Suppose you have a text file with the following contents:
	 * foo bar foo bar foo
	 * bar foo bar foo
	 *
	 * // Your pipeline
	 * DStream.ofType(String.class, "wc")
	 *     .flatMap(record -&gt; Stream.of(record.split("\\s+")))
	 *     .classify(word -&gt; word)
	 *
	 *  // Your Classifier is configured as 'dstream.grouper=FooBarClassifier'
	 *  // and its implementation looks like this:
	 *
	 * class FooBarClassifier implements Classifier {
	 *     public Integer apply(Object input) {
	 *         return input.equals("bar") ? 1 : 0;
	 *     }
	 *     . . .
	 * }
	 * </pre>
	 * The above would result in {@link DStream} which represents two classification groups:<br>
	 * Group-1 - bar bar bar bar<br>
	 * Group-2 - foo foo foo foo foo<br>
	 * Even though it would look continuous to you
	 * (i.e., bar bar bar bar foo foo foo foo foo).<br>
	 * <br>
	 * In the "distributable" reality, this often coincides with data <i>partitioning</i>, since
	 * {@link Classifier} is compliant with the general semantics of partitioners
	 * by returning an {@link Integer} representing classification id, which could be treated by a
	 * target partitioner as partition id.<br>
	 * <br>
	 * However, the actual <b><i>data partitioning</i></b> is the function of the system and
	 * exist primarily to facilitate greater parallelization when it comes to actual data processing.
	 * <b><i>Data classification</i></b> on the other hand, is the function of the application deriving its
	 * requirement from the use case at hand (e.g., group all 'foo's and 'bar's together).<br>
	 * So, it is important to separate the two, since it is quite conceivable that to facilitate greater
	 * parallelization in the truly distributed environment classification groups
	 * could be further partitioned (e.g., 2 groups into 8 partitions).<br>
	 * <br>
	 * Another configuration property relevant to this and every other <i>shuffle</i>-style operation
	 * is {@link DStreamConstants#PARALLELISM} which allows you to provide a hint as to the level of
	 * parallelisation you may want to accomplish and is typically passed as one of the constructor arguments
	 * to the instance of {@link Classifier}, but it could also be used by the target execution
	 * environment to configure its partitioner.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 *
	 * @param classifier function to extract value used by a target classifier to compute classification id.
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
	 * Returns a {@link DStream} consisting of the results of applying the given
	 * function on the entire {@link Stream} which typically represents a single partition/split
	 * handled by a currently executing task<br>. Essentially this is a gateway to use
	 * standard {@link Stream} API on a given piece of data represented by this stream.<br>
	 * Below is the variant of the rudimentary WordCount. Even though the API provides configuration
	 * for the implicit map-side combine, here you can see how something like explicit map-side combine
	 * could be accomplished via <i>compute</i> operation and standard {@link Stream} API.
	 * <pre>
	 * DStream.ofType(String.class, "wc")
	 *       .compute(stream -&gt; stream
	 *           .flatMap(line -&gt; Stream.of(line.split("\\s+")))
	 *           .collect(Collectors.toMap(word -&gt; word, word -&gt; 1, Integer::sum)).entrySet().stream()
	 *      ).reduceValues(word -&gt; word, word -&gt; 1, Integer::sum)
	 *  .executeAs("WordCount");
	 * </pre>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 *
	 * @param <R> The element type of the returned {@link DStream}
	 * @param computeFunction function to apply on the entire {@link Stream}.
	 * @return new {@link DStream} of type R
	 */
	<R> DStream<R> compute(SerFunction<? super Stream<A>, ? extends Stream<? extends R>> computeFunction);

	/**
	 * Performs a reduction on the elements of this stream, using an
	 * accumulation function returning a {@link DStream} with a single value
	 * of the same type as the source stream.<br>
	 * <br>
	 * This operation is a non-terminal equivalent of the
	 * <i>Stream.reduce(BinaryOperator)</i>.<br>
	 *
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 *
	 * @param accumulator a function for combining two values
	 * @return new {@link DStream} of the same type
	 */
	DStream<A> reduce(SerBinaryOperator<A> accumulator);

	/**
	 * Returns a {@link DStream} containing a single element which represents the
	 * minimum element of this stream according to the provided {@code SerComparator}.<br>
	 * <br>
	 * This operation is a non-terminal equivalent of the
	 * <i>Stream.min(Comparator)</i>.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 *
	 * @param comparator a stateless {@code SerComparator} to compare elements of this stream
	 * @return new {@link DStream} of the same type
	 */
	DStream<A> min(SerComparator<? super A> comparator);

	/**
	 * Returns a {@link DStream} containing a single element which represents the
	 * maximum element of this stream according to the provided {@code SerComparator}.<br>
	 * <br>
	 * This operation is a non-terminal equivalent of the
	 * <i>Stream.max(Comparator)</i>.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 *
	 * @param comparator a stateless {@code SerComparator} to compare elements of this stream
	 * @return new {@link DStream} of the same type
	 */
	DStream<A> max(SerComparator<? super A> comparator);

	/**
	 * Returns a stream consisting of the elements of this stream, sorted
	 * according to the provided {@code SerComparator}.
	 * <br>
	 * This operation is consistent with
	 * <i>Stream.sorted(Comparator)</i>.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>composable-transformation</i> operation.
	 * @param comparator a stateless {@code SerComparator} to be used to compare stream elements
	 * @return new {@link DStream} of the same type
	 */
	DStream<A> sorted(SerComparator<? super A> comparator);

	/**
	 * Returns a {@link DStream} of Key/Value pairs, where values mapped from the individual
	 * elements of this stream are grouped on the given <i>groupClassifier</i> (e.g., key) and
	 * reduced by the given <i>valueReducer</i>.<br>
	 * <br>
	 * This operation is a non-terminal equivalent of the
	 * <i>Stream.collect(Collectors.toMap(Function, Function, BinaryOperator))</i>.<br>
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
