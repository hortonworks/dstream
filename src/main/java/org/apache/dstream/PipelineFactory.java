package org.apache.dstream;

import java.net.URI;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.dstream.utils.Assert;

/**
 * Factory class to create execution {@link Pipeline}s.
 *
 */
public interface PipelineFactory {

	/**
	 * Convenience operator that declares that {@link Stream} used as source of a {@link Pipeline} 
	 * will be generated form the locations identified by {@link URI}s
	 * 
	 * Details related to actual creation of a {@link Stream} from provided {@link URI}s is undefined 
	 * and is handled by the providers available within the current execution environment.
	 *   
	 * @param paths
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Pipeline<T> from(URI... uris) {
		Assert.notEmpty(uris, "'uris' must not be null or empty");

		return (Pipeline<T>) new DefaultPipeline<T>(uris);
	}
	
	/**
	 * Convenience operator which allows you to provide implementations of {@link Supplier}s
	 * used by {@link Stream}s
	 * 
	 * @param streamSuppliers
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@SafeVarargs
	public static <T> Pipeline<T> from(Supplier<T>... streamSuppliers) {
		Assert.notEmpty(streamSuppliers, "'streamSuppliers' must not be null or empty");
		return (Pipeline<T>) new DefaultPipeline<T>(streamSuppliers);
	}
}
