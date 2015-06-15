package org.apache.dstream;


import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.support.Parallelizer;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Predicate;
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
 *     .combine(word -> word, word -> 1, Integer::sum)
 *  .executeAs("WordCount");
 * </pre>
 *
 * @param <T> the type of the stream elements
 */
public interface DistributableStream<T> extends DistributableExecutable<T>{

	/**
	 * Factory method which creates an instance of the {@code DistributableStream} of type T.
	 * 
	 * @param sourceItemType the type of the elements of this pipeline
	 * @param streamName the name of this stream
	 * @return the new {@link DistributableStream} of type T
	 * 
	 * @param <T> the type of pipeline elements
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributableStream<T> ofType(Class<T> sourceItemType, String streamName) {	
		return ExecutionSpecBuilder.getAs(sourceItemType, streamName, DistributableStream.class);
	}
	
	/*
	 * Elements that are semantically equivalent to the same of Stream API
	 */
	
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
	
	/*
	 * Elements that are semantically equivalent to the same of Stream API but may also have 
	 * distributable implications
	 */
	
//	/**
//	 * This operation maintains the same semantics as {@link Stream#distinct()} 
//	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.
//	 * 
//	 * This is an intermediate operation
//	 * 
//	 * @return
//	 */
//	DistributableStream<T> distinct();
//	
//	/**
//	 * This operation maintains the same semantics as {@link Stream#sorted()} 
//	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.
//	 * 
//	 * This is an intermediate operation
//	 * 
//	 * @return
//	 */
//	DistributableStream<T> sorted();
	
	/**
	 * Operation to provide a set of functions to create stream of Key/Values pairs 
	 * where all <i>values</i> associated with the same <i>key</i> are grouped 
	 * into a {@link List}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract classifier (e.g., key)
	 * @param valueMapper function to extract values
	 * 
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K, {@link List}&lt;V&gt;&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributableStream<Entry<K,Iterable<V>>> group(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper);
	
	/**
	 * Operation to provide a set of functions to create stream of Key/Values pairs 
	 * where all <i>values</i> associated with the same <i>key</i> are grouped 
	 * into a {@link List}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract classifier (e.g., key)
	 * @param valueMapper function to extract values
	 * @param parallelismSize size value to be used by default {@link Parallelizer}
	 * 
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K, {@link List}&lt;V&gt;&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributableStream<Entry<K,Iterable<V>>> group(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper, int parallelismSize);
	
	/**
	 * Operation to provide a set of functions to create stream of Key/Values pairs 
	 * where all <i>values</i> associated with the same <i>key</i> are grouped 
	 * into a {@link List}.<br>
	 * 
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract classifier (e.g., key)
	 * @param valueMapper function to extract values
	 * @param  parallelizer {@link Parallelizer} instance
	 * 
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K, {@link List}&lt;V&gt;&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributableStream<Entry<K,Iterable<V>>> group(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper, Parallelizer<T> parallelizer);
	
	/**
	 * Operation to provide a set of functions to create stream of Key/Value pairs 
	 * where all <i>values</i> associated with the same <i>key</i> are combined (reduced) 
	 * into a single value using provided combiner.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract classifier (e.g., key)
	 * @param valueMapper function to extract values
	 * @param combiner a merge function, to resolve collisions between
     *                      values associated with the same key
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K,V&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributableStream<Entry<K,V>> combine(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> combiner);
	
	/**
	 * Operation to provide a set of functions to create stream of Key/Value pairs 
	 * where all <i>values</i> associated with the same <i>key</i> are combined (reduced) 
	 * into a single value using provided combiner.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract classifier (e.g., key)
	 * @param valueMapper function to extract values
	 * @param combiner a merge function, to resolve collisions between
     *                      values associated with the same key
     * @param parallelismSize size value to be used by default {@link Parallelizer}
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K,V&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributableStream<Entry<K,V>> combine(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> combiner, int parallelismSize);
	
	/**
	 * Operation to provide a set of functions to create stream of Key/Value pairs 
	 * where all <i>values</i> associated with the same <i>key</i> are combined (reduced) 
	 * into a single value using provided combiner.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param classifier function to extract classifier (e.g., key)
	 * @param valueMapper function to extract values
	 * @param combiner a merge function, to resolve collisions between
     *                      values associated with the same key
     * @param parallelizer {@link Parallelizer} instance
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K,V&gt;
	 * 
	 * @param <K> classifier type (key)
	 * @param <V> value type
	 */
	<K,V> DistributableStream<Entry<K,V>> combine(Function<? super T, ? extends K> classifier, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> combiner, Parallelizer<T> parallelizer);
	
	/**
	 * Returns an equivalent stream while providing parallelization size directive.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation. 
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param parallelismSize size value to be used by default {@link Parallelizer}
	 * @return
	 */
	DistributableStream<T> parallel(int parallelismSize);
	
	/**
	 * Returns an equivalent stream while providing {@link Parallelizer}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param parallelizer {@link Parallelizer} instance
	 * @return
	 */
	DistributableStream<T> parallel(Parallelizer<T> parallelizer);	
	
	/**
	 * Operation to provide a set of functions to join data set represented by this {@link DistributableStream} 
	 * with another {@link DistributableStream} based on the common predicate (hash join).<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param streamP instance of {@link DistributableStream} to join with - (probe)
	 * @param hashKeyClassifier function to extract Key from this instance of the {@link DistributablePipeline} - (hash)
	 * @param hashValueMapper function to extract value from this instance of the {@link DistributablePipeline} - (hash)
	 * @param probeKeyClassifier function to extract Key from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @param probeValueMapper function to extract value from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K, {@link Pair}&lt;VL,VR&gt;&gt;
	 * 
	 * @param <TT> the type of elements of the {@link DistributableStream} to join with - (probe)
	 * @param <K>  the type of common classifier (key)
	 * @param <VH> the type of values of the elements extracted from this instance of the {@link DistributableStream} - hash
	 * @param <VP> the type of values of the elements extracted from the joined instance of the {@link DistributableStream} - probe
	 */
	<TT, K, VH, VP> DistributableStream<Entry<K, Pair<VH,VP>>> join(DistributableStream<TT> streamP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VH> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VP> probeValueMapper);
	
	/**
	 * Operation to provide a set of functions to join data set represented by this {@link DistributableStream} 
	 * with another {@link DistributableStream} based on the common predicate (hash join).<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param streamP instance of {@link DistributableStream} to join with - (probe)
	 * @param hashKeyClassifier function to extract Key from this instance of the {@link DistributablePipeline} - (hash)
	 * @param hashValueMapper function to extract value from this instance of the {@link DistributablePipeline} - (hash)
	 * @param probeKeyClassifier function to extract Key from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @param probeValueMapper function to extract value from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @param parallelismSize size value to be used by default {@link Parallelizer}
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K, {@link Pair}&lt;VL,VR&gt;&gt;
	 * 
	 * @param <TT> the type of elements of the {@link DistributableStream} to join with - (probe)
	 * @param <K>  the type of common classifier (key)
	 * @param <VH> the type of values of the elements extracted from this instance of the {@link DistributableStream} - hash
	 * @param <VP> the type of values of the elements extracted from the joined instance of the {@link DistributableStream} - probe
	 */
	<TT, K, VH, VP> DistributableStream<Entry<K, Pair<VH,VP>>> join(DistributableStream<TT> streamP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VH> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VP> probeValueMapper,
																	  int parallelismSize);
	
	/**
	 * Operation to provide a set of functions to join data set represented by this {@link DistributableStream} 
	 * with another {@link DistributableStream} based on the common predicate (hash join).<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is an <i>shuffle</i> operation.
	 * 
	 * @param streamP instance of {@link DistributableStream} to join with - (probe)
	 * @param hashKeyClassifier function to extract Key from this instance of the {@link DistributablePipeline} - (hash)
	 * @param hashValueMapper function to extract value from this instance of the {@link DistributablePipeline} - (hash)
	 * @param probeKeyClassifier function to extract Key from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @param probeValueMapper function to extract value from the joined instance of the {@link DistributablePipeline} - (probe)
	 * @param parallelizer {@link Parallelizer} instance
	 * @return {@link DistributableStream} of type {@link Entry}&lt;K, {@link Pair}&lt;VL,VR&gt;&gt;
	 * 
	 * @param <TT> the type of elements of the {@link DistributableStream} to join with - (probe)
	 * @param <K>  the type of common classifier (key)
	 * @param <VH> the type of values of the elements extracted from this instance of the {@link DistributableStream} - hash
	 * @param <VP> the type of values of the elements extracted from the joined instance of the {@link DistributableStream} - probe
	 */
	<TT, K, VH, VP> DistributableStream<Entry<K, Pair<VH,VP>>> join(DistributableStream<TT> streamP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VH> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VP> probeValueMapper,
																	  Parallelizer<T> parallelizer);
}
