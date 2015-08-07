/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.net.URI;

import org.junit.Test;

import dstream.support.SourceFilter;
import dstream.support.UriSourceSupplier;

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
