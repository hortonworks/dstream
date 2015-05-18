package org.apache.dstream;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Pair;

/**
 * 
 *
 * @param <T>
 * @param <K>
 * @param <H>
 * @param <P>
 */
public class PredicateJoinFunction<T,K,H,P> implements Function<Stream<T>, Stream<Entry<K, Pair<H,P>>>> {
	private static final long serialVersionUID = -6426897829643010018L;
	
	private final Function<Stream<?>,Stream<Entry<K,H>>> hashFunction; 
	
	@SuppressWarnings("rawtypes")
	private Function probeFunction;
	
	/**
	 * 
	 * @param keyExtractor
	 * @param valueExtractor
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	PredicateJoinFunction(KeyValueExtractorFunction hashFunction, KeyValueExtractorFunction probeFunction){
		this.hashFunction = (Function<Stream<?>, Stream<Entry<K, H>>>) hashFunction.compose(new KeyValuesStreamAggregatingFunction(null));
		this.probeFunction = probeFunction;
	}
	
	/**
	 * 
	 * @param aggregatingFunction
	 */
	@SuppressWarnings("unchecked")
	public void setProbeAggregator(Function<Stream<?>,Stream<?>> aggregatingFunction){
		this.probeFunction = this.probeFunction.compose(aggregatingFunction);
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Stream<Entry<K, Pair<H,P>>> apply(Stream<T> streamsToJoin) {
		Object[] streamArr = streamsToJoin.toArray();
		Stream<Entry<K,?>> hashStream = (Stream<Entry<K,?>>) streamArr[0];
		Stream<Entry<K,?>> probeStream = (Stream<Entry<K,?>>) streamArr[1];
		
		Map joined = this.hashFunction.apply(hashStream).collect(Collectors.toMap(s -> s.getKey(), s -> s.getValue()));

		((Function<Stream<?>, Stream<Entry<K,P>>>)this.probeFunction).apply(probeStream).forEach(item -> {
			joined.merge(item.getKey(), item.getValue(), (a, b) -> Pair.of(a, b));;
		});
		
		return joined.entrySet().stream();
	}
}
