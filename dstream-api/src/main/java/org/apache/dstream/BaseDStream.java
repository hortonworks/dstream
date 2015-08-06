package org.apache.dstream;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.SerBiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.SerFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerPredicate;
/**
 * Base strategy for {@link DStream} which contains all common operations.
 * 
 * @param <A> the type of the stream elements
 * @param <T>
 */
interface BaseDStream<A, T> extends DistributableExecutable<A> {

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
	 * @param stream
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
	 * @param stream
	 * @return new {@link DStream} of the same type.
	 */
	T unionAll(T stream);
	
	/**
	 * Returns a {@link DStream} consisting of the elements of this stream that match
     * the given predicate.<br>
     * Consistent with {@link Stream#filter(java.util.function.Predicate)}
     * 
	 * @param predicate predicate to apply to each element to determine if it
     *                  should be included
	 * @return new {@link DStream}
	 */
	T filter(SerPredicate<? super A> predicate);
	
	/**
	 * 
	 * @return
	 */
	T partition();
	
	/**
	 * 
	 * @param classifier
	 * @return
	 */
	T partition(SerFunction<? super A, ?> classifier);
	
	/**
	 * Returns a {@link DStream} consisting of the results of replacing each element of
     * this stream with the contents of a mapped stream produced by applying
     * the provided mapping function to each element.<br>
     * 
     * Consistent with {@link Stream#flatMap(java.util.function.Function)}
     * 
	 * @param mapper function to apply to each element which produces a stream
     *               of new values
	 * @return new {@link DStream}
	 */
	<R> DStream<R> flatMap(SerFunction<? super A, ? extends Stream<? extends R>> mapper);
	
	/**
	 * Returns a {@link DStream} consisting of the results of applying the given
     * function to the elements of this stream.<br>
     * 
     * Consistent with {@link Stream#map(java.util.function.Function)}
     * 
	 * @param mapper function to apply to each element
	 * @return new {@link DStream}
	 */
	<R> DStream<R> map(SerFunction<? super A, ? extends R> mapper);
	
	/**
	 * Returns a {@link DStream} consisting of the results of applying the given function
	 * on the entire stream (i.e., partition/split) represented as {@link Stream}.<br>
	 * This operation essentially allows to fall back on standard {@link Stream} API.
	 * 
	 * @param computeFunction function to apply on the entire {@link Stream}.
	 * @return new {@link DStream}
	 */
	<R> DStream<R> compute(SerFunction<? super Stream<A>, ? extends Stream<? extends R>> computeFunction);
	
	<K,V> DStream<Entry<K,V>> reduceGroups(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper,
			SerBinaryOperator<V> valueReducer);
	
	<K,V> DStream<Entry<K,List<V>>> aggregateGroups(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper);
	
	<K,V,F> DStream<Entry<K,F>> aggregateGroups(SerFunction<? super A, ? extends K> groupClassifier, 
			SerFunction<? super A, ? extends V> valueMapper,
			SerBiFunction<?,V,F> valueAggregator);
}
