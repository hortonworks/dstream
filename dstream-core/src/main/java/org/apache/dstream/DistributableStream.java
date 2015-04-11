package org.apache.dstream;


import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.BiFunction;
import org.apache.dstream.SerializableHelpers.BinaryOperator;
import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.SerializableHelpers.Predicate;

/**
 * A sequence of elements supporting sequential and distributable aggregate 
 * operations
 *
 * @param <T> the type of the stream elements
 */
public interface DistributableStream<T> extends Distributable<T>{

	/**
	 * Factory method which returns a sequential {@code DistributableStream} of 
	 * elements of the provided type. The source for the {@link DistributableStream} 
	 * could be provided at the configuration time by setting {@value #SRC_SUPPLIER} 
	 * or {@value #SRC_URL_SUPPLIER}. You can also provide the source by using 
	 * {@link #ofType(Class, Supplier)} factory method. 
	 * 
	 * 
	 * @param sourceItemType
	 * @return
	 */
	public static <T> DistributableStream<T> ofType(Class<T> sourceItemType) {	
		return ofType(sourceItemType, null);
	}
	
	/**
	 * Factory method which returns a sequential {@code DistributableStream} of 
	 * elements of the provided type and source of the stream supplied by 
	 * the {@link Supplier}
	 * 
	 * Custom suppliers could be provided allowing program arguments to be used in
	 * predicate logic to determine sources dynamically.
	 * 
	 * @param sourceItemType
	 * @param sourceSuppliers
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> DistributableStream<T> ofType(Class<T> sourceItemType, SourceSupplier<?> sourceSuppliers) {	
		return ADSTBuilder.getAs(sourceItemType, sourceSuppliers, DistributableStream.class);
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
	 */
	// Should we rename it to combine? Nothing is being reduced here. Possible reduction is a sideffect?
	<K,V> DistributableStream<Entry<K,V>> reduce(Function<? super T, ? extends K> keyMapper, 
			Function<? super T, ? extends V> valueMapper, 
			BinaryOperator<V> op);
	
	<TT,R> DistributableStream<R> join(DistributableStream<TT> streamR, 
			BiFunction<Stream<T>, Stream<TT>, Stream<R>> joinFunction);
	
	Stream<T> stream();
	
}
