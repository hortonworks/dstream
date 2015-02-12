package org.apache.dstream.io;

import java.util.stream.Stream;

import org.apache.dstream.Streamable;

public class TextFile<K,V> implements Streamable<V>{

	public static <K,V> TextFile<K,V> create(Class<K> keyClass, Class<V> valueClass, String path) {
		return new TextFile<K,V>();
	}

	@Override
	public Stream<V> toStream() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
