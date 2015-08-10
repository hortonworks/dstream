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
package dstream.local.ri;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.AbstractDStreamExecutionDelegate;
import dstream.DStreamInvocationPipeline;
import dstream.utils.Assert;

/**
 * 
 * @param <T>
 */
public class LocalDStreamExecutionDelegate<T> extends AbstractDStreamExecutionDelegate {
	
	@Override
	public Runnable getCloseHandler() {
		return new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, DStreamInvocationPipeline... invocationPipelines) {
		Assert.notEmpty(invocationPipelines, "'invocationPipelines' must not be null or empty");
		Assert.isTrue(invocationPipelines.length == 1, "Execution of DStreamInvocationPipeline groups is not supported at the moment");

		/*
		 * Assembles the list of executable Streams by applying user defined functionality 
		 */
		Stream<?> executableStreams = Stream.of(invocationPipelines)
			.map(pipeline -> new ExecutableStreamBuilder(executionName, pipeline, executionConfig).build());
		
		/*
		 * Executes assembled Streams producing the List of partitioned results
		 * The amount of elements (Streams) in the result list equals the amount of 
		 * result partitions.
		 */
		List<Stream<?>> results = executableStreams
				.collect(Collectors.toList()) 
				.stream()
				.flatMap(stream -> ((Stream<Stream<?>>) stream))
				.map(stream -> stream.collect(Collectors.toList()).stream())
				.collect(Collectors.toList());
	
		return results.stream();
	}
}
