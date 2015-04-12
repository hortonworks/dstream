package org.apache.dstream;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;

/**
 * Implementation of {@link StreamProcessingTask} which expects source {@link Stream} to  
 * contain elements of values grouped by key as in {@link Entry&lt;K,Iterator&lt;V&gt;&gt;}.
 * Grouped values are aggregated using provided 'streamAggregator'
 * 
 * Utility Function to compose final Function based on presence of 
 * 'streamProcessingFunction', which typically happens on each subsequence stage after reduce.
 * For example, stream.flatMap(..).map(..).reduce(..).fletMap(..).map(..)...
 * 
 * In the above there will be two stages. 1. flatMap(..).map(..)
 * 									      2. reduce(..) + fletMap(..).map(..).
 * The important part here is the 2nd stage where reduce(..) will be represented by the 'streamAggregator'
 * and 'streamProcessingFunction' is the subsequent fletMap(..).map(..). Also, the 'streamProcessingFunction'
 * is optional since it only exists if there are subsequent stream operation after 'streamAggregator'.
 * 
 * @param <K>
 * @param <V>
 * @param <KN>
 * @param <VN>
 */
class KeyValuesAggregatingStreamProcessingFunction<K,V,KN,VN> extends AbstractStreamProcessingFunction<Entry<K,Iterator<V>>, Entry<KN,VN>> {
	private static final long serialVersionUID = 761764186949445839L;

	/**
	 * 
	 * @param streamAggregator
	 */
	KeyValuesAggregatingStreamProcessingFunction(Function<Stream<Entry<K, Iterator<V>>>, Stream<Entry<K,V>>> streamAggregator) {
		this(null, streamAggregator);
	}
	
	/**
	 * 
	 * @param streamProcessingFunction
	 * @param streamAggregator
	 */
	@SuppressWarnings("unchecked")
	KeyValuesAggregatingStreamProcessingFunction(Function<Stream<Entry<K,V>>, Stream<Entry<KN,VN>>> streamProcessingFunction, 
			Function<Stream<Entry<K, Iterator<V>>>, Stream<Entry<K,V>>> streamAggregator) {	
		super(compose(streamAggregator, streamProcessingFunction));
	}
	
	/**
	 * 
	 * @param streamAggregator
	 * @param streamProcessingFunction
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Function compose(Function streamAggregator, Function streamProcessingFunction) {
		Assert.notNull(streamAggregator, "'streamAggregator' must not be null");
		return streamProcessingFunction == null ? streamAggregator : streamProcessingFunction.compose(streamAggregator);
	}
}
