package org.apache.dstream;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Pair;

public class PredicateJoiner<K,L,R> implements Function<Stream<Stream<?>>, Stream<Pair<L, R>>> {
	
	public PredicateJoiner(Function<? super L, ? extends K> lClassifier, Function<? super R, ? extends K> rClassifier){
		
	}

	@Override
	public Stream<Pair<L, R>> apply(Stream<Stream<?>> streamsToJoin) {
		Assert.notNull(streamsToJoin, "'streamsToJoin' must not be null");	
		List<Stream<?>> streamsList = streamsToJoin.collect(Collectors.toList());
		Assert.isTrue(streamsList.size() == 2, "'streamsToJoin' must contain 2 streams");
		
		Stream<L> left = (Stream<L>) streamsList.get(0);
		Stream<R> right = (Stream<R>) streamsList.get(1);
		
		
		return null;
	}

}
