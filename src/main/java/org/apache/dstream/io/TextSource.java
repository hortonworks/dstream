package org.apache.dstream.io;

import java.net.URI;
import java.util.stream.Stream;

public class TextSource<K,V> extends KeyValueStreamableSource<K, V> {
	

	protected TextSource(Class<K> keyClass, Class<V> valueClass, URI uri) {
		super(keyClass, valueClass, uri);
	}
	
	public static <K,V> TextSource<K,V> create(Class<K> keyClass, Class<V> valueClass, URI uri) {
		return new TextSource<K, V>(keyClass, valueClass, uri);
	}

	@Override
	public Stream<V> toStream() {
		// TODO Auto-generated method stub
		return null;
	}

}
