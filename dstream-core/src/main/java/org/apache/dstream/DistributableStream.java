package org.apache.dstream;


import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.BiFunction;
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
	
	/*
	 * Additional elements specific to distributable nature of this stream
	 */
	
	/**
	 * This operation maintains the similar semantics as {@link Collectors#toMap(java.util.function.Function, 
	 * java.util.function.Function, java.util.function.BinaryOperator)} while returning 
	 * {@link DistributableStream}
	 * 
	 * This is an intermediate operation
	 * 
	 * @param keyMapper
	 * @param valueMapper
	 * @param op
	 * @return
	 * 
	 * @param <K> key type 
	 * @param <V> value type 
	 */
	// Should we rename it to combine? Nothing is being reduced here. Possible reduction is a side-effect?
	<K,V> DistributableStream<Entry<K,V>> reduce(Function<? super T, ? extends K> keyMapper, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> op);
	
	/**
	 * Join based on common predicate
	 * 
	 * @param lKeyMapper
	 * @param lValueMapper
	 * @param pipelineR
	 * @param rKeyMapper
	 * @param rValueMapper
	 * @return
	 */
	<TT, K, VL, VR> DistributableStream<Entry<K, Pair<VL,VR>>> join(DistributableStream<TT> streamP,
																	  Function<? super T, ? extends K> hashKeyClassifier,
																	  Function<? super T, ? extends VL> hashValueMapper,
																	  Function<? super TT, ? extends K> probeKeyClassifier,
																	  Function<? super TT, ? extends VR> probeValueMapper);
}
