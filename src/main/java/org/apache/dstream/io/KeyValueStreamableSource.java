package org.apache.dstream.io;

import java.net.URI;
import java.util.Objects;

public abstract class KeyValueStreamableSource<K,V> implements StreamableSource<V> {
	protected final Class<K> keyClass;
	
	protected final Class<V> valueClass;
	
	protected final URI uri;
	
	/**
	 * 
	 * @param keyClass
	 * @param valueClass
	 * @param uri
	 */
	protected KeyValueStreamableSource(Class<K> keyClass, Class<V> valueClass, URI uri){
		Objects.requireNonNull(keyClass, "'keyClass' must not be null");
		Objects.requireNonNull(valueClass, "'valueClass' must not be null");
		Objects.requireNonNull(uri, "'uri' must not be null");
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.uri = uri;
	}
	
	@Override
	public URI getUri() {
		return this.uri;
	}
	
	public String toString(){
		return this.getClass().getSimpleName() + ":" + uri;
	}
}
