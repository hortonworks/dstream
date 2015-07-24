package org.apache.dstream.tez;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.KVUtils;
import org.apache.dstream.utils.Pair;

/**
 * 
 * @param <K>
 * @param <L>
 * @param <R>
 */
final class PredicateJoiner<K,L,R> implements Function<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -6370095851510630737L;

	private final Function<? super L, ? extends K> lClassifier;
	
	private final Function<? super R, ? extends K> rClassifier;
	
	private boolean applied;
	
	/**
	 * 
	 * @param lClassifier
	 * @param rClassifier
	 */
	PredicateJoiner(Function<? super L, ? extends K> lClassifier, Function<? super R, ? extends K> rClassifier){
		this.lClassifier = lClassifier;
		this.rClassifier = rClassifier;
	}

	/**
	 * 
	 */
	@Override
	public Stream<?> apply(Stream<Stream<?>> streamsToJoin) {
		Assert.notNull(streamsToJoin, "'streamsToJoin' must not be null");	
		List<Stream<?>> streamsList = streamsToJoin.collect(Collectors.toList());
		Assert.isTrue(streamsList.size() == 2, "'streamsToJoin' must contain 2 streams");
		
		return this.doApply(streamsList);
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Stream<?> doApply(List<Stream<?>> streamsList) {	
		Map joined = new HashMap();
		
		streamsList.forEach(stream -> {
			stream.forEach(s -> {
				Function f = applied ? rClassifier : lClassifier;
				Entry<Object, Iterator<?>> e = (Entry<Object, Iterator<?>>) s;
				if (e.getKey() != null){
					Object[] valuesGroup =  StreamSupport.stream(Spliterators.spliteratorUnknownSize(e.getValue(), Spliterator.ORDERED), false).toArray();
					Assert.isTrue(valuesGroup.length == 1, "Multiple values per key while no grouping function is provided");
					Entry flatnedEntry = KVUtils.kv(e.getKey(), valuesGroup[0]);
					Object key = f.apply(flatnedEntry);
					if (applied){
						joined.merge(key, flatnedEntry, Pair::of);
					}
					else {
						joined.put(key, flatnedEntry);
					}
				}
				else {
					StreamSupport.stream(Spliterators.spliteratorUnknownSize(e.getValue(), Spliterator.ORDERED), false)
						.forEach(value -> {
							if (applied){
								joined.merge(f.apply(value), value, Pair::of);
							}
							else {
								joined.put(f.apply(value), value);
							}
						});
				}
			});
			applied = true;
		});
		
		return joined.values().stream();
	}
}
