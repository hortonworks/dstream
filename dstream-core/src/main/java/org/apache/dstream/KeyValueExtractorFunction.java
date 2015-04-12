package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.KVUtils;

@SuppressWarnings("rawtypes")
class KeyValueExtractorFunction implements Function<Stream<?>, Stream<?>> {
	private static final long serialVersionUID = -4257572937412682381L;
	
	private final Function keyExtractor;
	private final Function valueExtractor;
	
	/**
	 * 
	 * @param keyExtractor
	 * @param valueExtractor
	 */
	KeyValueExtractorFunction(Function keyExtractor, Function valueExtractor){
		this.keyExtractor = keyExtractor;
		this.valueExtractor = valueExtractor;
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		return streamIn.map(val -> KVUtils.kv(this.keyExtractor.apply(val), this.valueExtractor.apply(val)));
	}
}
