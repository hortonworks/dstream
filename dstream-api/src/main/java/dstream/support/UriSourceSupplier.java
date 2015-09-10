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
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.DStreamConstants;
import dstream.utils.Assert;

/**
 * {@link URI}-based implementation of the {@link SourceSupplier}
 */
public final class UriSourceSupplier extends SourceSupplier<Stream<URI>> {
	private static final long serialVersionUID = -4643164807046654114L;

	private final URI[] uris;

	//private SourceFilter<Stream<URI>> sourceFilter;

	/**
	 * Validates that {@link URI} expressed as {@link String} is of proper
	 * format and could be converted to an instance of the {@link URI}.
	 */
	public static boolean isURI(String source){
		Pattern pattern = Pattern.compile("^[a-zA-Z0-9\\-_]+:");
		return pattern.matcher(source).find();
	}

	/**
	 * Converts {@link String} based representation of the {@link URI} to the actual
	 * instance of the {@link URI}
	 */
	public static URI toURI(String strURI){
		try {
			return new URI(strURI.trim());
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 *
	 */
	public UriSourceSupplier(Properties executionConfig, String executionGraphName){
		super(executionConfig, executionGraphName);
		String sourceProperty = executionConfig.getProperty(DStreamConstants.SOURCE + executionGraphName);
		Assert.notEmpty(sourceProperty, "'" + (DStreamConstants.SOURCE + executionGraphName) +  "' property can not be found in execution configuration file.");
		Assert.isTrue(UriSourceSupplier.isURI(sourceProperty), "When defaulting to UriSourceSupplier the system had determined "
				+ "that the value of the '" + (DStreamConstants.SOURCE + executionGraphName) + "' property is not a valid URI.\nWas '" + sourceProperty
				+ "'. \nExample of valid definition - "
				+ "'dstream.source.wc=file:${user.dir}/src/main/examples/dstream/examples/sample.txt' \nwhere 'wc' represents the pipeline "
				+ "name set during the initial construction of the pipeline.");
		List<URI> uris = Stream.of(sourceProperty.split(";")).map(uriStr -> UriSourceSupplier.toURI(uriStr)).collect(Collectors.toList());
		this.uris = uris.toArray(new URI[uris.size()]);
	}


	/**
	 *
	 */
	@Override
	public boolean equals(Object obj) {
		return (obj instanceof UriSourceSupplier &&
				Arrays.equals(((UriSourceSupplier)obj).get().toArray(), this.get().toArray()));
	}

	/**
	 * Returns an array of {@link URI} provided during the construction of this instance.
	 */
	@Override
	public Stream<URI> get() {
		return Stream.of(this.uris);
	}

	/**
	 *
	 */
	@Override
	public String toString(){
		return ":src" + Arrays.asList(this.uris).toString();
	}


	public void setSourceFilter(SourceFilter<Stream<URI>> sourceFilter) {
		// TODO Auto-generated method stub

	}

	public SourceFilter<Stream<URI>> getSourceFilter() {
		// TODO Auto-generated method stub
		return null;
	}
}
