package org.apache.dstream;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.KVUtils;

/**
 * Will aggregate values of a {@link Stream} who's elements are Key/Values pairs 
 * as in [K, Iterator[V]] using provided 'aggregationOperator' producing a new {@link Stream}
 * with [K,V] semantics.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KeyValuesStreamAggregatingFunction<K,V,T> implements Function<Stream<Entry<K,Iterator<V>>>,Stream<T>> {

	private static final long serialVersionUID = 1133920289646508908L;
	
	@SuppressWarnings("rawtypes")
	private final BinaryOperator aggregationOperator;
	
	/**
	 * 
	 * @param aggregationOperator
	 */
	/*
	 * BinaryOperator is type-less to accommodate BiFunctions (e.g. Aggregators::aggregate)
	 */
	@SuppressWarnings("rawtypes")
	public KeyValuesStreamAggregatingFunction(BinaryOperator aggregationOperator) {
		this.aggregationOperator = aggregationOperator;
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
	 */
	@SuppressWarnings("unchecked")
	private T mergeValuesForCurrentKey(Entry<K, Iterator<V>> currentEntry){
		Stream<V> valuesStream = (Stream<V>) StreamSupport.stream(Spliterators.spliteratorUnknownSize(currentEntry.getValue(), Spliterator.ORDERED), false);
		Object value = this.aggregationOperator == null ? valuesStream.findFirst().get() : KVUtils.kv(currentEntry.getKey(), valuesStream.reduce(this.aggregationOperator).get());
		return (T) value;
	}
}
