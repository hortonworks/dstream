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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import dstream.AbstractDStreamExecutionDelegate;
import dstream.StreamOperations;
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

	/**
	 * 
	 */
	@Override
	protected List<Stream<Stream<?>>> doExecute(String executionName, Properties executionConfig, StreamOperations... operationsGroups) {
		Assert.notEmpty(executionName, "'executionName' must not be null or empty");
		Assert.notNull(executionConfig, "'executionConfig' must not be null");
		Assert.notEmpty(operationsGroups, "'operationsGroups' must not be null or empty");
		Assert.isTrue(operationsGroups.length == 1, "Execution of StreamOperations groups is not supported at the moment");
			
		LocalDStreamExecutionEngine executionEngine = new LocalDStreamExecutionEngine();
		
		List<Stream<Stream<?>>> results = new ArrayList<>();
		
		for (StreamOperations operations : operationsGroups) {
			ExecutableStreamBuilder executionBuilder = new ExecutableStreamBuilder(executionName, operations, executionConfig);
			Stream<?> executableStream = executionBuilder.build();
			Stream<Stream<?>> result = executionEngine.execute(executableStream);
			results.add(result);
		}
		
		return results;
	}
}
