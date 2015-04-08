package org.apache.dstream.support;

import java.net.URI;
import java.util.Arrays;

import org.apache.dstream.SourceSupplier;
import org.apache.dstream.utils.Assert;

public class UriSourceSupplier implements SourceSupplier<URI> {
	
	private final URI[] uris;
	
	public UriSourceSupplier(URI... uris){
		Assert.notEmpty(uris, "'uris' must not be null or empty");
		this.uris = uris;
	}
	
	public static UriSourceSupplier from(URI... uris) {
		return new UriSourceSupplier(uris);
	}
	
	@Override
	public URI[] get() {
		return this.uris;
	}
	
	@Override
	public String toString(){
		return Arrays.asList(this.uris).toString();
	}
}
