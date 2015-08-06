package org.apache.dstream;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.SerBiFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.SerFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerPredicate;
/**
 * Base strategy for {@link DStream}
 * Contains all common operations
 *
 * @param <A>
 * @param <T>
 */
interface BaseDStream<A, T>  extends DistributableExecutable<A> {

	T union(T stream);
	
	T unionAll(T stream);
	
	T filter(SerPredicate<? super A> predicate);
	
	T partition();
	
	T partition(SerFunction<? super A, ?> classifier);
	
	
	<R> DStream<R> flatMap(SerFunction<? super A, ? extends Stream<? extends R>> mapper);
	
	<R> DStream<R> map(SerFunction<? super A, ? extends R> mapper);
	
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
