package org.apache.dstream;


import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Pair;

/**
 * A sequence of elements supporting sequential and distributable aggregate 
 * operations
 *
 * @param <T> the type of the stream elements
 */
public interface DistributableStream<T> extends DistributableExecutable<T>{

	/**
	 * Factory method which returns a sequential {@code DistributableStream} of 
	 * elements of the provided type and source of the stream supplied by 
	 * the {@link Supplier}
	 * 
	 * Custom suppliers could be provided allowing program arguments to be used in
	 * predicate logic to determine sources dynamically.
	 * 
	 * @param sourceItemType
	 * @param streamName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributableStream<T> ofType(Class<T> sourceItemType, String streamName) {	
		return ExecutionContextSpecificationBuilder.getAs(sourceItemType, streamName, DistributableStream.class);
	}
	
	/*
	 * Elements that are semantically equivalent to the same of Stream API
	 */
	
	/**
	 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.
	 * 
	 * This is an intermediate operation
     * 
	 * @param mapper
	 * @return
	 * 
	 * @param <R> the type of the elements of the new stream
	 */
	<R> DistributableStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.
	 * 
	 * This is an intermediate operation
	 * 
	 * @param mapper
	 * @return
	 * 
	 * @param <R> the type of the elements of the new stream
	 */
	<R> DistributableStream<R> map(Function<? super T, ? extends R> mapper);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
	 * with the exception of returning {@link DistributableStream} instead of the {@link Stream}.
	 * 
	 * This is an intermediate operation
	 * 
	 * @param predicate
	 * @return
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
	 * Will reduce all values for a given key to a single value using provided 
	 * {@link BinaryOperator} 
	 * 
	 * This is an intermediate operation
	 * 
	 * @param keyClassifier the classifier function mapping input elements to keys
	 * @param valueMapper a mapping function to produce values
	 * @param reducer a merge function, used to resolve collisions between
     *                      values associated with the same key
	 * @return the new {@link DistributableStream} of type {@link Entry}[K,V]
	 * 
	 * @param <K> key type 
	 * @param <V> value type 
	 */
	<K,V> DistributableStream<Entry<K,V>> reduce(Function<? super T, ? extends K> keyClassifier, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> reducer);
	
	/**
	 * Will join two {@link DistributableStream}s based on common predicate
	 * 
	 * @param streamP joining stream
	 * @param hashKeyClassifier hash side key mapper
	 * @param hashValueMapper hash side value mapper
	 * @param probeKeyClassifier probe side key mapper
	 * @param probeValueMapper probe side value mapper
	 * @return
	 * 
	 * @param <TT> the type of the joining stream
	 * @param <K> common key type
	 * @param <VH> hash side value type
	 * @param <VP> probe side value type
	 */
	<TT, K, VH, VP> DistributableStream<Entry<K, Pair<VH,VP>>> join(DistributableStream<TT> streamP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VH> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VP> probeValueMapper);
}
