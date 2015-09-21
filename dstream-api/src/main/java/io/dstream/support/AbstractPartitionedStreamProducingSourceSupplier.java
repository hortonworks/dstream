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
import java.util.stream.Stream;

/**
 * Primary goal of this implementation of {@link SourceSupplier} is to provide sub-classes with
 * current partition identifier enabling them to supply the correct {@link Stream}.
 * It does so by providing an additional {@link #doGet(int)} method which is called by
 * {@link #get()} after partition identifier is determined. It also makes {@link #get()}
 * 'final' ensuring there is no confusing which method must be implemented by sub-classes.
 *
 * @param <T> the type of elements in the returned {@link Stream}
 */
public abstract class AbstractPartitionedStreamProducingSourceSupplier<T> extends SourceSupplier<Stream<T>>{

	private static final long serialVersionUID = -7671596834377215486L;

	/**
	 *
	 */
	public AbstractPartitionedStreamProducingSourceSupplier(Properties executionConfig, String pipelineName) {
		super(executionConfig, pipelineName);
	}

	/**
	 * Returns the {@link Stream} representing current partition.
	 */
	@Override
	public final Stream<T> get() {
		return this.doGet(PartitionIdHelper.getPartitionId());
	}

	/**
	 * Returns the {@link Stream} representing partition identified by the 'partitionId'.
	 * @param partitionId partition identifier
	 * @return {@link Stream} representing partition identified by the 'partitionId'.
	 */
	protected abstract Stream<T> doGet(int partitionId) ;
}
