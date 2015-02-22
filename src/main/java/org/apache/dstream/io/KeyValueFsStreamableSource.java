package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @param <K>
 * @param <V>
 */
public abstract class KeyValueFsStreamableSource<K,V> implements StreamableSource<V>, FsStreamableSource<V> {
	protected final Class<K> keyClass;
	
	protected final Class<V> valueClass;
	
	protected final Path[] path;
	
	private final String schema;
	
	protected KeyValueFsStreamableSource(Class<K> keyClass, Class<V> valueClass, Supplier<Path[]> sourceSupplier){
		Objects.requireNonNull(keyClass, "'keyClass' must not be null");
		Objects.requireNonNull(valueClass, "'valueClass' must not be null");
		Objects.requireNonNull(sourceSupplier, "'sourceSupplier' must not be null");
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.path = sourceSupplier.get();
		this.schema = this.path[0].toUri().getScheme();
	}
	
	@Override
	public Path[] getPath() {
		return this.path;
	}
	
	@Override
	public String getScheme(){
		return this.schema;
	}
	
	public String toString(){
		return this.getClass().getSimpleName();
	}
}
