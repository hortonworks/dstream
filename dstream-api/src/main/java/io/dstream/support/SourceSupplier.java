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
package io.dstream.support;

import java.util.Properties;

import io.dstream.DStreamConstants;
import io.dstream.SerializableStreamAssets.SerSupplier;
import io.dstream.utils.Assert;
import io.dstream.utils.ReflectionUtils;


/**
 * Specialized definition of {@link SerSupplier} to return an array of sources of type T
 *
 * @param <T> the type of sources
 */
public abstract class SourceSupplier<T> implements SerSupplier<T> {
	private static final long serialVersionUID = 1041398921739932285L;

	protected final Properties executionConfig;

	protected final String pipelineName;

	/**
	 * Constructs this instance using execution configuration
	 * properties and the name of the pipeline to which it is supplying sources to.
	 * See {@link DStreamConstants#SOURCE} and {@link DStreamConstants#SOURCE_SUPPLIER}
	 * properties.
	 */
	public SourceSupplier(Properties executionConfig, String pipelineName) {
		Assert.notNull(executionConfig, "'executionConfig' must not be null");
		Assert.notEmpty(pipelineName, "'pipelineName' must not be null or empty");
		this.executionConfig = executionConfig;
		this.pipelineName = pipelineName;
	}

	/**
	 * Factory method that constructs the instance of the SourceSupplier by using
	 * execution configuration properties and the name of the pipeline to which it
	 * is supplying sources to.
	 * See {@link DStreamConstants#SOURCE} and {@link DStreamConstants#SOURCE_SUPPLIER}
	 * properties.
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
