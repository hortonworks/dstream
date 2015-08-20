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
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.DStreamConstants;
import dstream.function.SerializableFunctionConverters.SerSupplier;
import dstream.utils.Assert;


/**
 * Specialized definition of {@link SerSupplier} to return an array of sources of type T
 *
 * @param <T>
 */
public interface SourceSupplier<T> extends SerSupplier<T[]> {
	
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
	 * Factory method that creates an instance of the {@link SourceSupplier} from 
	 * the {@link DStreamConstants#SOURCE} property.<br>
	 * The value of the {@link DStreamConstants#SOURCE} property could be either a {@link URI} or
	 * the fully qualified class name of the {@link SourceSupplier} implementation, essentially 
	 * providing a mechanism to support multiple types of sources.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> SourceSupplier<T> create(String sourceProperty, SourceFilter<?> sourceFilter){
		Assert.notEmpty(sourceProperty, "'sourceProperty' must not be null or empty");
		try {
			SourceSupplier sourceSupplier;
			if (isURI(sourceProperty)){
				 List<URI> uris = Stream.of(sourceProperty.split(";")).map(uriStr -> SourceSupplier.toURI(uriStr)).collect(Collectors.toList());
				 sourceSupplier = UriSourceSupplier.from(uris.toArray(new URI[uris.size()]));
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
	 */
	void setSourceFilter(SourceFilter<T> sourceFilter);
	
	/**
	 * 
	 * @return
	 */
	SourceFilter<T> getSourceFilter();
}
