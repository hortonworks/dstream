package org.apache.dstream;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.utils.Assert;

/**
 * Implementation of {@link StreamProcessingTask} which expects source {@link Stream} to be 
 * contain elements of values grouped by key as in {@link Entry&lt;K,Iterator&lt;V&gt;&gt;}.
 * Grouped values are aggregated using provided 'streamAggregator'
 * 
 * @param <K>
 * @param <V>
 * @param <KN>
 * @param <VN>
 */
public class KeyValuesAggregatingStreamProcessingFunction<K,V,KN,VN> extends AbstractStreamProcessingFunction<Entry<K,Iterator<V>>, Entry<KN,VN>> {
	private static final long serialVersionUID = 761764186949445839L;

	public KeyValuesAggregatingStreamProcessingFunction(Function<Stream<Entry<K, Iterator<V>>>, Stream<Entry<K,V>>> streamAggregator) {
		this(null, streamAggregator);
	}
	
	@SuppressWarnings("unchecked")
	public KeyValuesAggregatingStreamProcessingFunction(Function<Stream<Entry<K,V>>, Stream<Entry<KN,VN>>> streamProcessingFunction, 
			Function<Stream<Entry<K, Iterator<V>>>, Stream<Entry<K,V>>> streamAggregator) {
		
		super(compose(streamAggregator, streamProcessingFunction));
	}
	
	/**
	 * Utility to compose two Functions
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Function compose(Function streamAggregator, Function streamProcessingFunction) {
		Assert.notNull(streamAggregator, "'streamAggregator' must not be null");
		if (streamProcessingFunction == null){
			return (Function) streamAggregator;
		} else {
			return (Function) streamProcessingFunction.compose(streamAggregator);
		}
	}
}
