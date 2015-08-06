package org.apache.dstream.function;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.function.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.KVUtils;

/**
 * Will combine (reduce) values of a {@link Stream} who's elements are Key/Values pairs 
 * as in [K, Iterator[V]] using provided 'combiner' producing a new {@link Stream}
 * with [K,V] semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ValuesReducingFunction<K,V,T> implements Function<Stream<Entry<K,Iterator<V>>>,Stream<T>> {
	private static final long serialVersionUID = 1133920289646508908L;
	
	@SuppressWarnings("rawtypes")
	private final BinaryOperator combiner;
	
	/**
	 * 
	 * @param aggregationOperator
	 */
	@SuppressWarnings("rawtypes")
	public ValuesReducingFunction(BinaryOperator combiner) {
		this.combiner = combiner;
	}

	/**
	 * 
	 */
	@Override
	public Stream<T> apply(Stream<Entry<K, Iterator<V>>> sourceStream) {
		return sourceStream.map(entry -> this.mergeValuesForCurrentKey(entry));
	}
	
	/**
	 * 
	 * @param valuesStream
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Object buildValue(Stream<V> valuesStream){
		return valuesStream.reduce((java.util.function.BinaryOperator<V>) this.combiner).get();
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private T mergeValuesForCurrentKey(Entry<K, Iterator<V>> currentEntry){
		Stream<V> valuesStream = (Stream<V>) StreamSupport.stream(Spliterators.spliteratorUnknownSize(currentEntry.getValue(), Spliterator.ORDERED), false);
		Object value = this.combiner == null ? valuesStream.findFirst().get() : KVUtils.kv(currentEntry.getKey(), this.buildValue(valuesStream));
		return (T) value;
	}
}
