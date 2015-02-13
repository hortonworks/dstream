package org.apache.dstream.io;

import java.net.URL;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.dstream.Streamable;

public class TextFile<K,V> implements Streamable<V> {
	
	private final Class<K> keyClass;
	
	private final Class<V> valueClass;
	
	private final URL url;

	public static <K,V> TextFile<K,V> create(Class<K> keyClass, Class<V> valueClass, URL url) {
		return new TextFile<K, V>(keyClass, valueClass, url);
	}
	
	private TextFile(Class<K> keyClass, Class<V> valueClass, URL url){
		Objects.requireNonNull(keyClass, "'keyClass' must not be null");
		Objects.requireNonNull(valueClass, "'valueClass' must not be null");
		Objects.requireNonNull(url, "'url' must not be null");
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.url = url;
	}

	@Override
	public Stream<V> toStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getUrl() {
		return this.url;
	}
	
	
}
