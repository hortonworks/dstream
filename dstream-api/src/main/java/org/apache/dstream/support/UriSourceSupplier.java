package org.apache.dstream.support;

import java.net.URI;
import java.util.Arrays;

import org.apache.dstream.utils.Assert;

/**
 * {@link URI}-based implementation of the {@link SourceSupplier}
 */
public class UriSourceSupplier implements SourceSupplier<URI> {
	private static final long serialVersionUID = -4643164807046654114L;
	
	private final URI[] uris;
	
	private SourceFilter<URI> sourceFilter;

	/**
	 * 
	 * @param uris
	 */
	public UriSourceSupplier(URI... uris){
		Assert.notEmpty(uris, "'uris' must not be null or empty");
		this.uris = uris;
	}
	
	/**
	 * 
	 * @param uris
	 * @return
	 */
	public static UriSourceSupplier from(URI... uris) {
		return new UriSourceSupplier(uris);
	}
	
	public boolean equals(Object obj) {
        return (obj instanceof UriSourceSupplier && 
        		Arrays.equals(((UriSourceSupplier)obj).get(), this.get()));
    }
	
	/**
	 * 
	 */
	@Override
	public URI[] get() {
		return this.uris;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return ":src" + Arrays.asList(this.uris).toString();
	}

	@Override
	public void setSourceFilter(SourceFilter<URI> sourceFilter) {
		this.sourceFilter = sourceFilter;
	}

	@Override
	public SourceFilter<URI> getSourceFilter() {
		return this.sourceFilter;
	}
}
