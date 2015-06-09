package org.apache.dstream;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.KVUtils;

/**
 * Implementation of {@link Function} to create {@link Stream} of Key/Value pairs from another {@link Stream}.
 * Key/Value pairs represented as {@link Entry} 
 * <br>
 * Key/Values are created using <i>keyExtractor</i> and <i>valueExtractor</i> provided 
 * during the construction.
 *
 * @param <T> the type of the source stream
 * @param <K> the key type
 * @param <V> the value type
 */
public class KeyValueMappingFunction<T,K,V> implements Function<Stream<T>, Stream<Entry<K, V>>> {
	private static final long serialVersionUID = -4257572937412682381L;
	
	private final Function<T, K> keyExtractor;
	
	private final Function<T, V> valueExtractor;
	
	private final BinaryOperator<V> combiner;
	
	/**
	 * 
	 * @param keyExtractor
	 * @param valueExtractor
	 */
	public KeyValueMappingFunction(Function<T, K> keyExtractor, Function<T, V> valueExtractor) {
		this(keyExtractor, valueExtractor, null);
	}
	
	/**
	 * 
	 * @param keyExtractor
	 * @param valueExtractor
	 * @param combiner
	 */
	public KeyValueMappingFunction(Function<T, K> keyExtractor, Function<T, V> valueExtractor, BinaryOperator<V> combiner) {
		Assert.notNull(keyExtractor, "'keyExtractor' must not be null");
		Assert.notNull(valueExtractor, "'valueExtractor' must not be null");
		
		this.keyExtractor = keyExtractor;
		this.valueExtractor = valueExtractor;
		this.combiner = combiner;
	}

	/**
	 * Will create a new {@link Stream} of Key/Value pairs represented as {@link Entry} 
	 */
	@Override
	public Stream<Entry<K, V>> apply(Stream<T> streamIn) {	
		Assert.notNull(streamIn, "'streamIn' must not be null");
		if (this.combiner != null){
			return streamIn.collect(Collectors.toMap(this.keyExtractor, this.valueExtractor, this.combiner)).entrySet().stream();
		}
		else {
			return streamIn.map(val -> KVUtils.kv(this.keyExtractor.apply(val), this.valueExtractor.apply(val)));
		}
	}
}
