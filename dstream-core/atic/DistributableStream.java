package org.apache.dstream;


import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.BiFunction;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Pair;

/**
 * Java {@link Stream}-style specialization strategy of {@link DistributableExecutable} 
 * which provides a view into distributable data set as sequence of elements of type T 
 * that support sequential and parallel aggregate operations.<br>
 * Also see {@link DistributablePipeline}.<br>
 * Below is the example of rudimentary <i>Word Count</i> written in this style:<br>
 * <pre>
 * DistributableStream.ofType(String.class, "wc")
 *     .flatMap(line -> Stream.of(line.split("\\s+")))
 *     .group(word -> word, word -> 1, Integer::sum)
 *  .executeAs("WordCount");
 * </pre>
 *
 * @param <T> the type of the stream elements
 */
public interface DistributableStream<T> extends DistributableExecutable<T> {

	/**
	 * Factory method which creates an instance of the {@code DistributableStream} of type T.
	 * 
	 * @param sourceItemType the type of the elements of this pipeline
	 * @param sourceIdentifier the value which will be used in conjunction with 
	 *                       {@link DistributableConstants#SOURCE} in configuration 
	 *                       to point to source of this stream 
	 *                       (e.g., dstream.source.foo=file://foo.txt where 'foo' is the <i>sourceProperty</i>)
	 * @return the new {@link DistributableStream} of type T
	 * 
	 * @param <T> the type of pipeline elements
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributableStream<T> ofType(Class<T> sourceElementType, String sourceIdentifier) {	
		Assert.notNull(sourceElementType, "'sourceElementType' must not be null");
		Assert.notEmpty(sourceIdentifier, "'sourceIdentifier' must not be null or empty");
		return StreamOperationsCollector.as(sourceElementType, sourceIdentifier, DistributableStream.class);
	}
	
	/**
	 * Operation to provide a {@link Function} to operate on the entire {@link Stream} representing individual partition.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
	 * @return {@link DistributablePipeline} of type R
	 * 
	 * @param <R> the type of the elements of the new pipeline
	 */
	<R> DistributableStream<R> compute(Function<? super Stream<T>, ? extends Stream<? extends R>> computeFunction);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
     * 
	 * @param mapper function to apply to each element which produces a stream
     *               of new values
	 * @return {@link DistributableStream} of type R
	 * 
	 * @param <R> the type of the elements of the new stream
	 */
	<R> DistributableStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param mapper function to apply to each element
	 * @return {@link DistributableStream} of type R
	 * 
	 * @param <R> the type of the elements of the new stream
	 */
	<R> DistributableStream<R> map(Function<? super T, ? extends R> mapper);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param predicate predicate to apply to each element to determine if it
     *                  should be included
	 * @return {@link DistributableStream} of type T
	 */
	DistributableStream<T> filter(Predicate<? super T> predicate);
	
	
	/**
	 * Will group values mapped from individual elements into an {@link Iterable} based on the provided 
	 * <i>groupClassifier</i> returning a new stream of grouped elements mapped to a 'groupClassifier' 
	 * as Key/Value pairs.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param groupClassifier
	 * @param valueMapper
	 * @return
	 */
	<K,V> DistributableStream<Entry<K,Iterable<V>>> group(Function<? super T, ? extends K> groupClassifier, 
			Function<? super T, ? extends V> valueMapper);
	
	/**
	 * Will group values mapped from individual elements of this stream based on the provided <i>groupClassifier</i>,
	 * reducing grouped values using provided <i>valueReducer</i> returning a new stream of reduced values mapped 
	 * to a result of 'groupClassifier' as Key/Value pairs.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param groupClassifier
	 * @param valueMapper
	 * @param valueReducer
	 * @return
	 */
	// the same semantics as Reduce for groups, meaning reducing being a BinaryOperator aggregates to a single value
	<K,V> DistributableStream<Entry<K,V>> reduceGroups(Function<? super T, ? extends K> groupClassifier, 
			Function<? super T, ? extends V> valueMapper,
			BinaryOperator<V> valueReducer);
	
	/**
	 * Will group values mapped from individual elements of this stream based on the provided <i>groupClassifier</i>,
	 * aggregating grouped values using provided <i>valueAggregator</i> returning a new stream of aggregated values 
	 * mapped to a result of 'groupClassifier' as Key/Value pairs.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param groupClassifier
	 * @param valueMapper
	 * @param valueAggregator
	 * @return
	 */
	// BinaryFunction "is" BinaryOperator hence this method could be used in place of reduce
	<K,V,D> DistributableStream<Entry<K,D>> aggregateGroups(Function<? super T, ? extends K> groupClassifier, 
			Function<? super T, ? extends V> valueMapper,
			BiFunction<?,V,D> valueAggregator);
	
	/**
	 * Will join two streams together using provided <i>joinFunction</i> returning a new stream with elements of type D.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param streamR
	 * @param joinFunction
	 * @return
	 */
	<K,R> DistributableStream<Pair<T, R>> join(DistributableStream<R> streamR, Function<? super T, ? extends K> lClassifier, Function<? super R, ? extends K> rClassifier);
	
	/**
	 * Will join two or more streams together using provided <i>joinFunction</i> returning a new stream with elements 
	 * of type D.
	 * The <i>joinFunction</i> is a {@link Function} who's input parameter is {@link Stream} of {@link Stream}s allowing 
	 * joins on more then one stream.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param streams
	 * @param joinFunction
	 * @return
	 */
	<R,D> DistributableStream<D> join(DistributableStream<?>[] streams, Function<Stream<Stream<?>>, Stream<D>> joinFunction);
	
	/**
	 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
	 * Each element of this stream will be used by the target partitioner to calculate the partition id.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @return
	 */
	DistributableStream<T> partition();
	
	/**
	 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
	 * The value extracted by the <i>classifier</i> function from the elements of this stream will be used by the 
	 * target partitioner to calculate the partition id.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param classifier
	 * @return
	 */
	DistributableStream<T> partition(Function<? super T, ?> classifier);
	
	/**
	 * Returns an equivalent stream with sorted elements.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @return
	 */
	SortedDistributableStream<T> sorted();
	
	/**
	 * Returns an equivalent stream with elements sorted using the provided <i>comparator</i>.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @param comparator
	 * @return
	 */
	SortedDistributableStream<T> sorted(Comparator<? super T> comparator);
	
	
	
	/**
	 * Returns an equivalent stream filtering out duplicate elements.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @return
	 */
	DistributableStream<T> distinct();
	
	/**
	 * Will reduce the contents of this stream to a single value of type T using provided <i>reducer</i>.<br>
	 * <br>
	 * This is an <i>terminal</i> operation.
	 * 
	 * @param reducer
	 * @return
	 */
	Optional<T> reduce(BinaryOperator<T> reducer);
	
	/**
	 * Will reduce the contents of this stream by calculating the <i>min</i> element of type T using provided <i>comparator</i>.<br>
	 * <br>
	 * This is an <i>terminal</i> operation.
	 * 
	 * @param comparator
	 * @return
	 */
	Optional<T> min(Comparator<? super T> comparator);
	
	/**
	 * Will reduce the contents of this stream by calculating the <i>max</i> element of type T using provided <i>comparator</i>.<br>
	 * <br>
	 * This is an <i>terminal</i> operation.
	 * 
	 * @param comparator
	 * @return
	 */
	Optional<T> max(Comparator<? super T> comparator);
	
	/**
	 * Will reduce the contents of this stream by counting all elements in this stream.<br>
	 * <br>
	 * This is an <i>terminal</i> operation.
	 * 
	 * @return
	 */
	long count();
}
