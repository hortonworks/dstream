package org.apache.dstream.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.net.URI;

import org.junit.Test;

public class UriSourceSupplierTests {

	@Test(expected=IllegalArgumentException.class)
	public void failWithEmptyConstructur() throws Exception{
		new UriSourceSupplier();
	}
	
	@Test
	public void validateWithoutSourceFilter() throws Exception{
		UriSourceSupplier supplier = new UriSourceSupplier(new URI("file:/foo"), new URI("http://foo.com"));
		assertEquals(new URI("file:/foo"), supplier.get()[0]);
		assertEquals(new URI("http://foo.com"), supplier.get()[1]);
		assertNull(supplier.getSourceFilter());
	}
	
	@Test
	public void validateWithSourceFilter() throws Exception{
		UriSourceSupplier supplier = new UriSourceSupplier(new URI("file:/foo"), new URI("http://foo.com"));
		SampleURISourceFilter filter = new SampleURISourceFilter();
		supplier.setSourceFilter(filter);
		assertEquals(new URI("file:/foo"), supplier.get()[0]);
		assertEquals(new URI("http://foo.com"), supplier.get()[1]);
		assertNotNull(supplier.getSourceFilter());
		assertSame(filter, supplier.getSourceFilter());
	}
	
	private static class SampleURISourceFilter implements SourceFilter<URI> {
		@Override
		public boolean accept(URI source) {
			return false;
		}
	}
}
