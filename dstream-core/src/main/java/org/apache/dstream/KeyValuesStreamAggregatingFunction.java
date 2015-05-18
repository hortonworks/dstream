package org.apache.dstream;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.KVUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private final Logger logger = LoggerFactory.getLogger(KeyValuesStreamAggregatingFunction.class);
	
	private final BinaryOperator<V> aggregationOperator;
	
	/**
	 * 
	 * @param aggregationOperator
	 */
	public KeyValuesStreamAggregatingFunction(BinaryOperator<V> aggregationOperator) {
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
		try {
			Stream<V> valuesStream = (Stream<V>) StreamSupport.stream(Spliterators.spliteratorUnknownSize(currentEntry.getValue(), Spliterator.ORDERED), false);
			Object value = this.aggregationOperator == null ? valuesStream.findFirst().get() : KVUtils.kv(currentEntry.getKey(), valuesStream.reduce(this.aggregationOperator).get());
			return (T) value;
		} 
		catch (Exception e) {
			logger.error("Failed to merge values for key " + currentEntry.getKey(), e);
			throw new IllegalStateException(e);
		}
	}
}
