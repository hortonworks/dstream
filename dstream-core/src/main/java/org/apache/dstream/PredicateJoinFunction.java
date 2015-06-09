package org.apache.dstream;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Pair;

/**
 * Implementation of {@link Function} which performs predicate-based join between 
 * the two {@link Stream}s.
 * <br>
 * It is initialized with <i>hashFunction</i> and <i>probeFunction</i> which are both 
 * of type {@link KeyValueMappingFunction} and produce {@link Stream}s of Key/Value pairs
 * with common KEY which is used as predicate to perform the actual join.
 * <br>
 * The actual join results in the new {@link Entry} with common key and values as a {@link Pair}
 * of with first element in the pair being the <i>hash</i> value and second being the 
 * <i>probe</i> value.
 * 
 * @param <T> the type of elements of source stream
 * @param <K> the key type of the {@link Entry} element of the result stream
 * @param <H> the type of the hash elements
 * @param <P> the type of the probe elements
 */
public class PredicateJoinFunction<K,H,P> implements Function<Stream<Stream<? extends Entry<K,? extends Object>>>, Stream<Entry<K, Pair<H,P>>>> {
	private static final long serialVersionUID = -6426897829643010018L;
	
	@SuppressWarnings("rawtypes")
	private Function hshKvMapper; 
	
	@SuppressWarnings("rawtypes")
	private Function probeKvMapper;
	
	/**
	 * 
	 * @param hashFunction creates Key/Value pairs representing the <i>hash</i> side of the join.
	 * @param probeFunction creates Key/Value pairs representing the <i>probe</i> side of the join.
	 */
	@SuppressWarnings("rawtypes")
	PredicateJoinFunction(KeyValueMappingFunction hashKvMapper, KeyValueMappingFunction probeKvMapper){
		Assert.notNull(hashKvMapper, "'hashKvMapper' must not be null");
		Assert.notNull(probeKvMapper, "'probeKvMapper' must not be null");
		
		this.hshKvMapper = hashKvMapper;
		this.probeKvMapper = probeKvMapper;
	}
	
	/**
	 * Composes additional function into <i>probeFunction</i>.
	 * 
	 * An example of such composition would be composing an aggregation function
	 * to be executed before the original <i>probeFunction</i>.
	 * 
	 * @param function
	 */
	@SuppressWarnings("unchecked")
	public void composeIntoProbe(Function<Stream<? extends Object>,Stream<? extends Object>> function){
		if (function != null){
			this.probeKvMapper = this.probeKvMapper.compose(function);
		}
	}
	
	/**
	 * 
	 * @param function
	 */
	@SuppressWarnings("unchecked")
	public void composeIntoHash(Function<Stream<?>,Stream<?>> function){
		if (function != null){
			this.hshKvMapper = this.hshKvMapper.compose(function);
		}
	}

	/**
	 * Will join two streams (<i>hash</i> and <i>probe</i>) together into a single stream of 
	 * {@link Entry}ies with common key and a {@link Pair} of values with 
	 * first element in the pair being the <i>hash</i> value and second being the <i>probe</i> value.
	 * 
	 * @param streamsToJoin
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Stream<Entry<K, Pair<H,P>>> apply(Stream<Stream<? extends Entry<K,? extends Object>>> streamsToJoin) {
		Assert.notNull(streamsToJoin, "'streamsToJoin' must not be null");	
		List<Stream<? extends Entry<K,? extends Object>>> streamsList = streamsToJoin.collect(Collectors.toList());
		Assert.isTrue(streamsList.size() == 2, "'streamsToJoin' must contain 2 streams");
		
		//TODO plug in implementation of the spillable map		
		Map joined = ((Stream<Entry<K,H>>)this.hshKvMapper.apply(streamsList.get(0))).collect(Collectors.toMap(he -> he.getKey(), he -> he.getValue()));
		
		((Function<Stream<?>, Stream<Entry<K,P>>>)this.probeKvMapper).apply(streamsList.get(1))
				.forEach(pe -> joined.merge(pe.getKey(), pe.getValue(), Pair::of));
	
		return joined.entrySet().stream();
	}
}
