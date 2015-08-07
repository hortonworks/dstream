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
