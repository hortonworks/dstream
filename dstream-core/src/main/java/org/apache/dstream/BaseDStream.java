package org.apache.dstream;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.BiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;

interface BaseDStream<A, T>  extends DistributableExecutable<A> {

	T union(T stream);
	
	T unionAll(T stream);
	
	T filter(Predicate<? super A> predicate);
	
	T partition();
	
	T partition(Function<? super A, ?> classifier);
	
	
	<R> DStream<R> flatMap(Function<? super A, ? extends Stream<? extends R>> mapper);
	
	<R> DStream<R> map(Function<? super A, ? extends R> mapper);
	
	<R> DStream<R> compute(Function<? super Stream<A>, ? extends Stream<? extends R>> computeFunction);
	
	<K,V> DStream<Entry<K,V>> reduceGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper,
			BinaryOperator<V> valueReducer);
	
	<K,V> DStream<Entry<K,List<V>>> aggregateGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper);
	
	<K,V,F> DStream<Entry<K,F>> aggregateGroups(Function<? super A, ? extends K> groupClassifier, 
			Function<? super A, ? extends V> valueMapper,
			BiFunction<?,V,F> valueAggregator);
}
