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

import java.net.URI;
import java.util.Arrays;

import dstream.utils.Assert;

/**
 * {@link URI}-based implementation of the {@link SourceSupplier}
 */
class UriSourceSupplier implements SourceSupplier<URI> {
	private static final long serialVersionUID = -4643164807046654114L;
	
	private final URI[] uris;
	
	private SourceFilter<URI> sourceFilter;

	/**
	 * 
	 */
	private UriSourceSupplier(URI... uris){
		Assert.notEmpty(uris, "'uris' must not be null or empty");
		this.uris = uris;
	}
	
	/**
	 * Factory method which constructs this instance with an array of {@link URI}s.
	 * The array argument must not be <i>null</i> or empty.
	 */
	public static UriSourceSupplier from(URI... uris) {
		return new UriSourceSupplier(uris);
	}
	
	/**
	 * 
	 */
	public boolean equals(Object obj) {
        return (obj instanceof UriSourceSupplier && 
        		Arrays.equals(((UriSourceSupplier)obj).get(), this.get()));
    }
	
	/**
	 * Returns an array of {@link URI} provided during the construction of this instance.
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

	/**
	 * Sets the instance of the {@link SourceFilter} which filters
	 * the {@link URI}s held by this instance.
	 */
	@Override
	public void setSourceFilter(SourceFilter<URI> sourceFilter) {
		this.sourceFilter = sourceFilter;
	}

	/**
	 * Returns an instance of the {@link SourceFilter} which filters
	 * the {@link URI}s held by this instance.
	 */
	@Override
	public SourceFilter<URI> getSourceFilter() {
		return this.sourceFilter;
	}
}
