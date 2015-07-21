package org.apache.dstream;

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

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.KVUtils;
import org.apache.dstream.utils.Pair;

public class PredicateJoiner<K,L,R> implements Function<Stream<Stream<?>>, Stream<Pair<L, R>>> {
	
	private final Function<? super L, ? extends K> lClassifier;
	
	private final Function<? super R, ? extends K> rClassifier;
	
	public PredicateJoiner(Function<? super L, ? extends K> lClassifier, Function<? super R, ? extends K> rClassifier){
		this.lClassifier = lClassifier;
		this.rClassifier = rClassifier;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<Pair<L, R>> apply(Stream<Stream<?>> streamsToJoin) {
		Assert.notNull(streamsToJoin, "'streamsToJoin' must not be null");	
		List<Stream<?>> streamsList = streamsToJoin.collect(Collectors.toList());
		Assert.isTrue(streamsList.size() == 2, "'streamsToJoin' must contain 2 streams");
		
		Stream<L> left = (Stream<L>) streamsList.get(0);
		Stream<R> right = (Stream<R>) streamsList.get(1);
		
		
		Map joined = new HashMap();
		
		left.forEach(s -> {
			Entry<Object, Iterator> e = (Entry<Object, Iterator>) s;
			if (e.getKey() != null){
				Stream<?> hashValuesStream =  StreamSupport.stream(Spliterators.spliteratorUnknownSize(e.getValue(), Spliterator.ORDERED), false);
				Object[] hashValuesGroup = hashValuesStream.toArray();
				Assert.isTrue(hashValuesGroup.length == 1, "Hash values contains multiple values per key while no grouping function is provided");
				Entry hashEntry = KVUtils.kv(e.getKey(), hashValuesGroup[0]);
				Object key = this.lClassifier.apply((L) hashEntry);
				joined.put(key, hashEntry);
			}
			else {
				while (e.getValue().hasNext()){
					Object value = e.getValue().next();
					Object key = this.lClassifier.apply((L) value);
					joined.put(key, value);
				}
			}
		});
		
		right.forEach(p -> {
			Entry<Object, Iterator<?>> pe = (Entry<Object, Iterator<?>>) p;
			if (pe.getKey() != null){
				Stream<?> probeValuesStream =  StreamSupport.stream(Spliterators.spliteratorUnknownSize(pe.getValue(), Spliterator.ORDERED), false);
				Object[] probeValuesGroup = probeValuesStream.toArray();
				Assert.isTrue(probeValuesGroup.length == 1, "Probe values contains multiple values per key while no grouping function is provided");
				Entry probeEntry = KVUtils.kv(pe.getKey(), probeValuesGroup[0]);
				Object key = this.rClassifier.apply((R) probeEntry);
				joined.merge(key, probeEntry, Pair::of);
			}
			else {
				while (pe.getValue().hasNext()){
					Object value = pe.getValue().next();
					Object key = this.rClassifier.apply((R) value);
					joined.put(key, value);
				}
			}
		});
		
		return joined.values().stream();
	}

}
