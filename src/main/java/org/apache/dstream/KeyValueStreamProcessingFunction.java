package org.apache.dstream;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;

public class KeyValueStreamProcessingFunction<K,V,KN,VN> extends AbstractStreamProcessingFunction<Entry<K, V>, Entry<KN,VN>> {

	private static final long serialVersionUID = 1107771730281461210L;

	public KeyValueStreamProcessingFunction(Function<Stream<Entry<K, V>>, Stream<Entry<KN, VN>>> streamProcessingFunction) {
		super(streamProcessingFunction);
	}
}
