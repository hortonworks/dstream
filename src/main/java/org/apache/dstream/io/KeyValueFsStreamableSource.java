package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.Objects;

/**
 * @param <K>
 * @param <V>
 */
public abstract class KeyValueFsStreamableSource<K,V> implements StreamableSource<V>, FsStreamableSource<V> {
	protected final Class<K> keyClass;
	
	protected final Class<V> valueClass;
	
	protected final Path path;
	
	/**
	 * 
	 * @param keyClass
	 * @param valueClass
	 * @param uri
	 */
	protected KeyValueFsStreamableSource(Class<K> keyClass, Class<V> valueClass, Path path){
		Objects.requireNonNull(keyClass, "'keyClass' must not be null");
		Objects.requireNonNull(valueClass, "'valueClass' must not be null");
		Objects.requireNonNull(path, "'path' must not be null");
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.path = path;
	}
	
	@Override
	public Path getPath() {
		return this.path;
	}
	
	public String toString(){
		return this.getClass().getSimpleName() + ":" + path.toUri();
	}
}
