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
import java.util.Properties;

import dstream.DStreamConstants;
import dstream.SerializableStreamAssets.SerSupplier;
import dstream.utils.ReflectionUtils;


/**
 * Specialized definition of {@link SerSupplier} to return an array of sources of type T
 *
 * @param <T>
 */
public abstract class SourceSupplier<T> implements SerSupplier<T> {
	private static final long serialVersionUID = 1041398921739932285L;

	protected final Properties executionConfig;

	protected final String executionGraphName;

	/**
	 *
	 */
	public SourceSupplier(Properties executionConfig, String executionGraphName) {
		this.executionConfig = executionConfig;
		this.executionGraphName = executionGraphName;
	}

	/**
	 * Factory method that creates an instance of the {@link SourceSupplier} from
	 * the {@link DStreamConstants#SOURCE} property.<br>
	 * The value of the {@link DStreamConstants#SOURCE} property could be either a {@link URI} or
	 * the fully qualified class name of the {@link SourceSupplier} implementation, essentially
	 * providing a mechanism to support multiple types of sources.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> SourceSupplier<T> create(Properties executionConfig, String pipelineName, SourceFilter<?> sourceFilter){
		try {
			Class<? extends SourceSupplier<?>> sourceSupplierClass;
			if (executionConfig.containsKey(DStreamConstants.SOURCE_SUPPLIER + pipelineName)){
				sourceSupplierClass = (Class<? extends SourceSupplier<?>>) Class
						.forName(executionConfig.getProperty(DStreamConstants.SOURCE_SUPPLIER + pipelineName), false, Thread.currentThread().getContextClassLoader());
			}
			else {
				sourceSupplierClass = UriSourceSupplier.class;
			}

			SourceSupplier sourceSupplier = ReflectionUtils.newInstance(sourceSupplierClass,
					new Class[]{Properties.class, String.class}, new Object[]{executionConfig, pipelineName});
			return sourceSupplier;
		}
		catch (Exception e) {
			throw new IllegalStateException("Failure while creating SurceSupplier.", e);
		}
	}
}
