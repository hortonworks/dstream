package org.apache.dstream;

import java.util.Map.Entry;
import java.util.stream.Stream;

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
		
	@SuppressWarnings("unchecked")
	public static <T> DStream<T> ofType(Class<T> sourceElementType, String sourceIdentifier) {	
		Assert.notNull(sourceElementType, "'sourceElementType' must not be null");
		Assert.notEmpty(sourceIdentifier, "'sourceIdentifier' must not be null or empty");
		return DStreamOperationsCollector.as(sourceElementType, sourceIdentifier, DStream.class);
	}
	
	DStream<A> filter(Predicate<? super A> predicate);
	
	<R> DStream<R> flatMap(Function<? super A, ? extends Stream<? extends R>> mapper);
	
	<R> DStream<R> map(Function<? super A, ? extends R> mapper);
	
	<R> DStream<R> compute(Function<? super Stream<A>, ? extends Stream<? extends R>> computeFunction);
	
	// maintains teh same semantics as reduce (reducing to singular). A Streaming version of collect(Collectors.toMap)
	<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper,
			BinaryOperator<V> valueReducer);
	
	// BinaryFunction "is" BinaryOperator hence this method could be used in place of reduce
	<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper,
			BiFunction<?,V,F> valueAggregator);
	
	<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper);
	
	<_A> DStream2<A,_A> join(DStream<_A> ds, Predicate<Tuple2<A,_A>> predicate);
	
	<_A,_B> DStream3<A,_A,_B> join(DStream2<_A,_B> ds, Predicate<Tuple3<A,_A,_B>> predicate);
	
	<_A,_B,_C> DStream4<A,_A,_B,_C> join(DStream3<_A,_B,_C> ds, Predicate<Tuple4<A,_A,_B,_C>> predicate);
	
	<_A,_B,_C,_D> DStream5<A,_A,_B,_C,_D> join(DStream4<_A,_B,_C,_D> ds, Predicate<Tuple5<A,_A,_B,_C,_D>> predicate);
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 */
	interface DStream2<A,B> extends DistributableExecutable<Tuple2<A,B>>{
		DStream2<A,B> filter(Predicate<? super Tuple2<A,B>> predicate);	
		<R> DStream<R> flatMap(Function<? super Tuple2<A,B>, ? extends Stream<? extends R>> mapper);	
		<R> DStream<R> map(Function<? super Tuple2<A, B>, ? extends R> mapper);
		<R> DStream<R> compute(Function<? super Stream<Tuple2<A, B>>, ? extends Stream<? extends R>> computeFunction);
		// maintains teh same semantics as reduce (reducing to singular). A Streaming version of collect(Collectors.toMap)
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple2<A, B>, ? extends K> groupClassifier, 
				Function<? super Tuple2<A, B>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		// BinaryFunction "is" BinaryOperator hence this method could be used in place of reduce
		<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super Tuple2<A, B>, ? extends K> groupClassifier, 
				Function<? super Tuple2<A, B>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		<_A> DStream3<A,B,_A> join(DStream<_A> ds, Predicate<Tuple3<A,B,_A>> predicate);
		<_A,_B> DStream4<A,B,_A,_B> join(DStream2<_A,_B> ds, Predicate<Tuple4<A,B,_A,_B>> predicate);
		<_A,_B,_C> DStream5<A,B,_A,_B,_C> join(DStream3<_A,_B,_C> ds, Predicate<Tuple5<A,B,_A,_B,_C>> predicate);
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 */
	interface DStream3<A,B,C> extends DistributableExecutable<Tuple3<A,B,C>> {
		DStream3<A,B,C> filter(Predicate<? super Tuple3<A,B,C>> predicate);	
		<R> DStream<R> flatMap(Function<? super Tuple3<A,B,C>, ? extends Stream<? extends R>> mapper);	
		<R> DStream<R> map(Function<? super Tuple3<A,B,C>, ? extends R> mapper);		
		<R> DStream<R> compute(Function<? super Stream<Tuple3<A,B,C>>, ? extends Stream<? extends R>> computeFunction);
		// maintains teh same semantics as reduce (reducing to singular). A Streaming version of collect(Collectors.toMap)
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple3<A,B,C>, ? extends K> groupClassifier, 
				Function<? super Tuple3<A,B,C>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		// BinaryFunction "is" BinaryOperator hence this method could be used in place of reduce
		<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super Tuple3<A,B,C>, ? extends K> groupClassifier, 
				Function<? super Tuple3<A,B,C>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		<_A> DStream4<A,B,C,_A> join(DStream<_A> ds, Predicate<Tuple4<A,B,C,_A>> predicate);
		<_A,_B> DStream5<A,B,C,_A,_B> join(DStream2<_A,_B> ds, Predicate<Tuple5<A,B,C,_A,_B>> predicate);
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 * @param <D>
	 */
	interface DStream4<A,B,C,D> extends DistributableExecutable<Tuple4<A,B,C,D>> {
		DStream4<A,B,C,D> filter(Predicate<? super Tuple4<A,B,C,D>> predicate);	
		<R> DStream<R> flatMap(Function<? super Tuple4<A,B,C,D>, ? extends Stream<? extends R>> mapper);	
		<R> DStream<R> map(Function<? super Tuple4<A,B,C,D>, ? extends R> mapper);
		<R> DStream<R> compute(Function<? super Stream<Tuple4<A,B,C,D>>, ? extends Stream<? extends R>> computeFunction);
		// maintains teh same semantics as reduce (reducing to singular). A Streaming version of collect(Collectors.toMap)
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple4<A,B,C,D>, ? extends K> groupClassifier, 
				Function<? super Tuple4<A,B,C,D>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		// BinaryFunction "is" BinaryOperator hence this method could be used in place of reduce
		<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super Tuple4<A,B,C,D>, ? extends K> groupClassifier, 
				Function<? super Tuple4<A,B,C,D>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
		
		<_A> DStream5<A,B,C,D,_A> join(DStream<_A> ds, Predicate<Tuple5<A,B,C,D,_A>> predicate);
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
		DStream5<A,B,C,D,E> filter(Predicate<? super Tuple5<A,B,C,D,E>> predicate);	
		<R> DStream<R> flatMap(Function<? super Tuple5<A,B,C,D,E>, ? extends Stream<? extends R>> mapper);	
		<R> DStream<R> map(Function<? super Tuple5<A,B,C,D,E>, ? extends R> mapper);
		<R> DStream<R> compute(Function<? super Stream<Tuple5<A,B,C,D,E>>, ? extends Stream<? extends R>> computeFunction);
		// maintains teh same semantics as reduce (reducing to singular). A Streaming version of collect(Collectors.toMap)
		<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super Tuple5<A,B,C,D,E>, ? extends K> groupClassifier, 
				Function<? super Tuple5<A,B,C,D,E>, ? extends V> valueMapper,
				BinaryOperator<V> valueReducer);	
		// BinaryFunction "is" BinaryOperator hence this method could be used in place of reduce
		<K,V,F> DStream<Entry<K,D>> aggregateGroups(Function<? super Tuple5<A,B,C,D,E>, ? extends K> groupClassifier, 
				Function<? super Tuple5<A,B,C,D,E>, ? extends V> valueMapper,
				BiFunction<?,V,F> valueAggregator);
		
		<K,V> DStream<Entry<K,Iterable<V>>> group(Function<? super A, ? extends K> groupClassifier, 
				Function<? super A, ? extends V> valueMapper);
	}
}
