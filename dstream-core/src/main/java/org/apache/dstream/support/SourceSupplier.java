package org.apache.dstream.support;

import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Supplier;
import org.apache.dstream.utils.Assert;


/**
 * Specialized definition of {@link Supplier} to return an array of sources of type T
 *
 * @param <T>
 */
public interface SourceSupplier<T> extends Supplier<T[]> {
	/**
	 * 
	 * @param source
	 * @return
	 */
	public static boolean isURI(String source){
		Pattern pattern = Pattern.compile("^[a-zA-Z0-9\\-_]+:");
		long count = Stream.of(source.split(";")).filter(src -> !pattern.matcher(src).find()).count();
		return count == 0;
	}
	
	/**
	 * 
	 * @param strURI
	 * @return
	 */
	public static URI toURI(String strURI){
		try {
			return new URI(strURI);
		} 
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * 
	 * @param sourceProperty
	 * @param sourceFilter
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> SourceSupplier<T> create(String sourceProperty, SourceFilter<?> sourceFilter){
		Assert.notEmpty(sourceProperty, "'sourceProperty' must not be null or empty");
		try {
			SourceSupplier sourceSupplier;
			if (isURI(sourceProperty)){
				 List<URI> uris = Stream.of(sourceProperty.split(";")).map(uriStr -> SourceSupplier.toURI(uriStr)).collect(Collectors.toList());
				 sourceSupplier = new UriSourceSupplier(uris.toArray(new URI[uris.size()]));
			}
			else {
				sourceSupplier = (SourceSupplier) Class.forName(sourceProperty, false, Thread.currentThread().getContextClassLoader()).newInstance();
			}
			sourceSupplier.setSourceFilter(sourceFilter);
			return sourceSupplier;
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create SourceSupplier", e);
		}
	}
	
	/**
	 * 
	 * @param sourceFilter
	 */
	void setSourceFilter(SourceFilter<T> sourceFilter);
	
	/**
	 * 
	 * @return
	 */
	SourceFilter<T> getSourceFilter();
}
