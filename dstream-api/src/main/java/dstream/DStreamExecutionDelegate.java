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
package dstream;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Strategy to implement delegates to execute {@link DStream}s.
 */
public interface DStreamExecutionDelegate {

	/**
	 * Executes {@link DStreamOperations} group returning a {@link Future} of the results.
	 * 
	 * @param executionName the name of this execution
	 * @param executionConfig execution configuration properties
	 * @param operationsGroups array of {@link DStreamOperations} to execute
	 * @return an array of {@link Stream}&lt;{@link Stream}&lt;?&gt;&gt; where each outer 
	 * {@link Stream} represents the result of execution of individual {@link ExecutionSpec}.<br>
	 * 
	 */
	// add comment that while signature allows for async invocation, the actual style could still be controlled by the implementation
	Future<Stream<Stream<?>>> execute(String executionName, Properties executionConfig, DStreamOperations... operationsGroups);
	
	/**
	 * Returns {@link Runnable} which contains logic relevant to closing of the result {@link Stream}.
	 * The returned {@link Runnable} will be executed when resulting {@link Stream#close()} is called.
	 * 
	 * @return
	 */
	Runnable getCloseHandler();
}
