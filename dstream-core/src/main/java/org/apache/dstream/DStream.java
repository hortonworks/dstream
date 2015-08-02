package org.apache.dstream;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DStream.DStream2.DStream2WithPredicate;
import org.apache.dstream.DStream.DStream3.DStream3WithPredicate;
import org.apache.dstream.DStream.DStream4.DStream4WithPredicate;
import org.apache.dstream.DStream.DStream5.DStream5WithPredicate;
import org.apache.dstream.function.SerializableFunctionConverters.BiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.apache.dstream.utils.Tuples.Tuple3;
import org.apache.dstream.utils.Tuples.Tuple4;
import org.apache.dstream.utils.Tuples.Tuple5;

public interface DStream<A> extends DistributableExecutable<A>{
		
	/**
	 * Factory method which creates an instance of the {@code DStream} of type T.
	 * 
	 * @param sourceItemType the type of the elements of this pipeline
	 * @param sourceIdentifier the value which will be used in conjunction with 
	 *                       {@link DistributableConstants#SOURCE} in configuration 
	 *                       to point to source of this stream 
	 *                       (e.g., dstream.source.foo=file://foo.txt where 'foo' is the <i>sourceIdentifier</i>)
	 * @return the new {@link DStream} of type T
	 * 
	 * @param <T> the type of pipeline elements
	 */
	@SuppressWarnings("unchecked")
	public static <T> DStream<T> ofType(Class<T> sourceElementType, String sourceIdentifier) {	
		Assert.notNull(sourceElementType, "'sourceElementType' must not be null");
		Assert.notEmpty(sourceIdentifier, "'sourceIdentifier' must not be null or empty");
		return DStreamOperationsCollector.as(sourceElementType, sourceIdentifier, DStream.class);
	}
	
	/**
	 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
	 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param predicate predicate to apply to each element to determine if it
     *                  should be included
	 * @return {@link DStream} of type T
	 */
	DStream<A> filter(Predicate<? super A> predicate);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
	 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
     * 
	 * @param mapper function to apply to each element which produces a stream
     *               of new values
	 * @return {@link DStream} of type R
	 * 
	 * @param <R> the type of the elements of the new stream
	 */
	<R> DStream<R> flatMap(Function<? super A, ? extends Stream<? extends R>> mapper);
	
	/**
	 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
	 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param mapper function to apply to each element
	 * @return {@link DStream} of type R
	 * 
	 * @param <R> the type of the elements of the new stream
	 */
	<R> DStream<R> map(Function<? super A, ? extends R> mapper);
	
	/**
	 * Operation to provide a {@link Function} to operate on the entire {@link Stream} representing 
	 * individual partition.<br>
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * 
	 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
	 * @return {@link DStream} of type R
	 * 
	 * @param <R> the type of the elements of the new pipeline
	 */
	<R> DStream<R> compute(Function<? super Stream<A>, ? extends Stream<? extends R>> computeFunction);
	
	/**
	 * Will group values mapped from individual elements (via valueMapper) based on the 
	 * provided <i>groupClassifier</i>, reducing grouped values using provided <i>valueReducer</i> 
	 * returning a new Key/Value stream with reduced values<br>
	 * Maintains semantics similar to Stream.collect(Collectors.toMap)
	 * 
	 * @param groupClassifier
	 * @param valueMapper
	 * @param valueReducer
	 * @return
	 */
	<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper,
			BinaryOperator<V> valueReducer);

	/**
	 * Similar to {@link #reduceGroups(Function, Function, BinaryOperator)} will group values 
	 * mapped from individual elements (via valueMapper) based on the 
	 * provided <i>groupClassifier</i>, aggregating grouped values using provided <i>valueAggregator</i> 
	 * returning a new Key/Value stream with aggregated values<br>
	 * 
	 * @param groupClassifier
	 * @param valueMapper
	 * @param valueAggregator
	 * @return
	 */
	<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper,
			BiFunction<?,V,F> valueAggregator);
	
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
	<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper);
	
	/**
	 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
	 * Each element of this stream will be used by the target partitioner to calculate the partition id.<br>
	 * The size of partitions for each shuffle stage is configured using {@link DistributableConstants#PARALLELISM}
	 * property in execution configuration.
	 * <br>
	 * This is an <i>intermediate</i> operation.
	 * <br>
	 * This is a <i>shuffle</i> operation.
	 * 
	 * @return
	 */
	<K,V> DStream<A> partition();
	
	<_A> DStream2WithPredicate<A,_A> join(DStream<_A> ds);
	
	<_A,_B> DStream3WithPredicate<A,_A,_B> join(DStream2<_A,_B> ds);
	
	<_A,_B,_C> DStream4WithPredicate<A,_A,_B,_C> join(DStream3<_A,_B,_C> ds);
	
	<_A,_B,_C,_D> DStream5WithPredicate<A,_A,_B,_C,_D> join(DStream4<_A,_B,_C,_D> ds);
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 */
	interface DStream2<A,B> extends DistributableExecutable<Tuple2<A,B>> {
		interface DStream2WithPredicate<A,B> extends DStream2<A,B>{
			DStream2<A,B> on(Predicate<? super Tuple2<A,B>> predicate);	
		}
		/**
		 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
		 * with the exception of returning {@link DStream2} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param predicate predicate to apply to each element to determine if it
	     *                  should be included
		 * @return {@link DStream2} of type T
		 */
		DStream2<A,B> filter(Predicate<? super Tuple2<A,B>> predicate);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
	     * 
		 * @param mapper function to apply to each element which produces a stream
	     *               of new values
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> flatMap(Function<? super Tuple2<A,B>, ? extends Stream<? extends R>> mapper);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param mapper function to apply to each element
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> map(Function<? super Tuple2<A, B>, ? extends R> mapper);
		
		/**
		 * Operation to provide a {@link Function} to operate on the entire {@link Stream} representing 
		 * individual partition.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new pipeline
		 */
		<R> DStream<R> compute(Function<? super Stream<Tuple2<A, B>>, ? extends Stream<? extends R>> computeFunction);
		/**
		 * Will group values mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, reducing grouped values using provided <i>valueReducer</i> 
		 * returning a new Key/Value stream with reduced values<br>
		 * Maintains semantics similar to Stream.collect(Collectors.toMap)
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueReducer
		 * @return
		 */
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple2<A, B>, ? extends K> groupClassifier, 
				Function<? super Tuple2<A, B>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		
		/**
		 * Similar to {@link #reduceGroups(Function, Function, BinaryOperator)} will group values 
		 * mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, aggregating grouped values using provided <i>valueAggregator</i> 
		 * returning a new Key/Value stream with aggregated values<br>
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueAggregator
		 * @return
		 */
		<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super Tuple2<A, B>, ? extends K> groupClassifier, 
				Function<? super Tuple2<A, B>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
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
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		/**
		 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
		 * Each element of this stream will be used by the target partitioner to calculate the partition id.<br>
		 * The size of partitions for each shuffle stage is configured using {@link DistributableConstants#PARALLELISM}
		 * property in execution configuration.
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>shuffle</i> operation.
		 * 
		 * @return
		 */
		DStream2<A,B> partition();
		
		<_A> DStream3WithPredicate<A,B,_A> join(DStream<_A> ds);
		<_A,_B> DStream4WithPredicate<A,B,_A,_B> join(DStream2<_A,_B> ds);
		<_A,_B,_C> DStream5WithPredicate<A,B,_A,_B,_C> join(DStream3<_A,_B,_C> ds);
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 */
	interface DStream3<A,B,C> extends DistributableExecutable<Tuple3<A,B,C>> {
		interface DStream3WithPredicate<A,B,C> extends DStream3<A,B,C>{
			DStream3<A,B,C> on(Predicate<? super Tuple3<A,B,C>> predicate);	
		}
		/**
		 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
		 * with the exception of returning {@link DStream3} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param predicate predicate to apply to each element to determine if it
	     *                  should be included
		 * @return {@link DStream3} of type T
		 */
		DStream3<A,B,C> filter(Predicate<? super Tuple3<A,B,C>> predicate);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
	     * 
		 * @param mapper function to apply to each element which produces a stream
	     *               of new values
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> flatMap(Function<? super Tuple3<A,B,C>, ? extends Stream<? extends R>> mapper);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param mapper function to apply to each element
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> map(Function<? super Tuple3<A,B,C>, ? extends R> mapper);		
		
		/**
		 * Operation to provide a {@link Function} to operate on the entire {@link Stream} representing 
		 * individual partition.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new pipeline
		 */
		<R> DStream<R> compute(Function<? super Stream<Tuple3<A,B,C>>, ? extends Stream<? extends R>> computeFunction);
		
		/**
		 * Will group values mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, reducing grouped values using provided <i>valueReducer</i> 
		 * returning a new Key/Value stream with reduced values<br>
		 * Maintains semantics similar to Stream.collect(Collectors.toMap)
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueReducer
		 * @return
		 */
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple3<A,B,C>, ? extends K> groupClassifier, 
				Function<? super Tuple3<A,B,C>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		
		/**
		 * Similar to {@link #reduceGroups(Function, Function, BinaryOperator)} will group values 
		 * mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, aggregating grouped values using provided <i>valueAggregator</i> 
		 * returning a new Key/Value stream with aggregated values<br>
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueAggregator
		 * @return
		 */
		<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super Tuple3<A,B,C>, ? extends K> groupClassifier, 
				Function<? super Tuple3<A,B,C>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
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
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		/**
		 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
		 * Each element of this stream will be used by the target partitioner to calculate the partition id.<br>
		 * The size of partitions for each shuffle stage is configured using {@link DistributableConstants#PARALLELISM}
		 * property in execution configuration.
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>shuffle</i> operation.
		 * 
		 * @return
		 */
		DStream3<A,B,C> partition();
		
		<_A> DStream4WithPredicate<A,B,C,_A> join(DStream<_A> ds);
		<_A,_B> DStream5WithPredicate<A,B,C,_A,_B> join(DStream2<_A,_B> ds);
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 * @param <D>
	 */
	interface DStream4<A,B,C,D> extends DistributableExecutable<Tuple4<A,B,C,D>> {
		interface DStream4WithPredicate<A,B,C,D> extends DStream4<A,B,C,D>{
			DStream4<A,B,C,D> on(Predicate<? super Tuple4<A,B,C,D>> predicate);	
		}
		/**
		 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
		 * with the exception of returning {@link DStream4} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param predicate predicate to apply to each element to determine if it
	     *                  should be included
		 * @return {@link DStream4} of type T
		 */
		DStream4<A,B,C,D> filter(Predicate<? super Tuple4<A,B,C,D>> predicate);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
	     * 
		 * @param mapper function to apply to each element which produces a stream
	     *               of new values
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> flatMap(Function<? super Tuple4<A,B,C,D>, ? extends Stream<? extends R>> mapper);
		
		/**
		 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param mapper function to apply to each element
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> map(Function<? super Tuple4<A,B,C,D>, ? extends R> mapper);
		
		/**
		 * Operation to provide a {@link Function} to operate on the entire {@link Stream} representing 
		 * individual partition.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new pipeline
		 */
		<R> DStream<R> compute(Function<? super Stream<Tuple4<A,B,C,D>>, ? extends Stream<? extends R>> computeFunction);
		
		/**
		 * Will group values mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, reducing grouped values using provided <i>valueReducer</i> 
		 * returning a new Key/Value stream with reduced values<br>
		 * Maintains semantics similar to Stream.collect(Collectors.toMap)
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueReducer
		 * @return
		 */
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple4<A,B,C,D>, ? extends K> groupClassifier, 
				Function<? super Tuple4<A,B,C,D>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		
		/**
		 * Similar to {@link #reduceGroups(Function, Function, BinaryOperator)} will group values 
		 * mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, aggregating grouped values using provided <i>valueAggregator</i> 
		 * returning a new Key/Value stream with aggregated values<br>
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueAggregator
		 * @return
		 */
		<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super Tuple4<A,B,C,D>, ? extends K> groupClassifier, 
				Function<? super Tuple4<A,B,C,D>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
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
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		/**
		 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
		 * Each element of this stream will be used by the target partitioner to calculate the partition id.<br>
		 * The size of partitions for each shuffle stage is configured using {@link DistributableConstants#PARALLELISM}
		 * property in execution configuration.
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>shuffle</i> operation.
		 * 
		 * @return
		 */
		DStream4<A,B,C,D> partition();
		
		<_A> DStream5WithPredicate<A,B,C,D,_A> join(DStream<_A> ds);
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 * @param <D>
	 * @param <E>
	 */
	interface DStream5<A,B,C,D,E>  extends DistributableExecutable<Tuple5<A,B,C,D,E>> {
		interface DStream5WithPredicate<A,B,C,D,E> extends DStream5<A,B,C,D,E>{
			DStream5<A,B,C,D,E> on(Predicate<? super Tuple5<A,B,C,D,E>> predicate);	
		}
		/**
		 * This operation maintains the same semantics as {@link Stream#filter(java.util.function.Predicate)} 
		 * with the exception of returning {@link DStream5} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param predicate predicate to apply to each element to determine if it
	     *                  should be included
		 * @return {@link DStream5} of type T
		 */
		DStream5<A,B,C,D,E> filter(Predicate<? super Tuple5<A,B,C,D,E>> predicate);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#flatMap(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
	     * 
		 * @param mapper function to apply to each element which produces a stream
	     *               of new values
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> flatMap(Function<? super Tuple5<A,B,C,D,E>, ? extends Stream<? extends R>> mapper);	
		
		/**
		 * This operation maintains the same semantics as {@link Stream#map(java.util.function.Function)} 
		 * with the exception of returning {@link DStream} instead of the {@link Stream}.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param mapper function to apply to each element
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new stream
		 */
		<R> DStream<R> map(Function<? super Tuple5<A,B,C,D,E>, ? extends R> mapper);
		
		/**
		 * Operation to provide a {@link Function} to operate on the entire {@link Stream} representing 
		 * individual partition.<br>
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * 
		 * @param computeFunction a mapping function to map {@link Stream}&lt;T&gt; to {@link Stream}&lt;R&gt;.
		 * @return {@link DStream} of type R
		 * 
		 * @param <R> the type of the elements of the new pipeline
		 */
		<R> DStream<R> compute(Function<? super Stream<Tuple5<A,B,C,D,E>>, ? extends Stream<? extends R>> computeFunction);
		
		/**
		 * Will group values mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, reducing grouped values using provided <i>valueReducer</i> 
		 * returning a new Key/Value stream with reduced values<br>
		 * Maintains semantics similar to Stream.collect(Collectors.toMap)
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueReducer
		 * @return
		 */
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple5<A,B,C,D,E>, ? extends K> groupClassifier, 
				Function<? super Tuple5<A,B,C,D,E>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		
		/**
		 * Similar to {@link #reduceGroups(Function, Function, BinaryOperator)} will group values 
		 * mapped from individual elements (via valueMapper) based on the 
		 * provided <i>groupClassifier</i>, aggregating grouped values using provided <i>valueAggregator</i> 
		 * returning a new Key/Value stream with aggregated values<br>
		 * 
		 * @param groupClassifier
		 * @param valueMapper
		 * @param valueAggregator
		 * @return
		 */
		<K,V,F> DStream<Entry<K,D>> aggregateGroups(Function<? super Tuple5<A,B,C,D,E>, ? extends K> groupClassifier, 
				Function<? super Tuple5<A,B,C,D,E>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
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
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		/**
		 * Returns an equivalent stream while hinting the underlying system to partition data represented by this stream.
		 * Each element of this stream will be used by the target partitioner to calculate the partition id.<br>
		 * The size of partitions for each shuffle stage is configured using {@link DistributableConstants#PARALLELISM}
		 * property in execution configuration.
		 * <br>
		 * This is an <i>intermediate</i> operation.
		 * <br>
		 * This is a <i>shuffle</i> operation.
		 * 
		 * @return
		 */
		DStream5<A,B,C,D,E> partition();
	}
}
