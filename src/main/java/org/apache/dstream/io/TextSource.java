package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.stream.Stream;

public class TextSource<K,V> extends KeyValueFsStreamableSource<K, V> {
	

	protected TextSource(Class<K> keyClass, Class<V> valueClass, Path path) {
		super(keyClass, valueClass, path);
	}
	
	public static <K,V> TextSource<K,V> create(Class<K> keyClass, Class<V> valueClass, Path path) {
		return new TextSource<K, V>(keyClass, valueClass, path);
	}

	@Override
	public Stream<V> toStream() {
		// TODO Auto-generated method stub
		return null;
	}
}
