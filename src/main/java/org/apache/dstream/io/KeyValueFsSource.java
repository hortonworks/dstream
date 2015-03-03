package org.apache.dstream.io;

import java.nio.file.Path;
import java.util.function.Supplier;

import org.apache.dstream.utils.Assert;

/**
 * @param <K>
 * @param <V>
 */
public abstract class KeyValueFsSource<K,V> implements FsSource<V> {
	protected final Class<K> keyClass;
	
	protected final Class<V> valueClass;
	
	protected final Path[] path;
	
	private final String schema;
	
	protected KeyValueFsSource(Class<K> keyClass, Class<V> valueClass, Supplier<Path[]> sourceSupplier){
		Assert.notNull(keyClass, "'keyClass' must not be null");
		Assert.notNull(valueClass, "'valueClass' must not be null");
		Assert.notNull(sourceSupplier, "'sourceSupplier' must not be null");
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.path = sourceSupplier.get();
		Assert.notNull(this.path, "'sourceSupplier' resulted in null paths");
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
		return this.getClass().getSimpleName() + "; scheme:[" + this.getScheme() + "];";
	}
	
//	@Override
//	public DistributedPipeline<V> asPipeline(String jobName){
//		DefaultDistributedPipeline<V> distributableSource = new DefaultDistributedPipeline<>(jobName);
//		return distributableSource;
//	}
}
