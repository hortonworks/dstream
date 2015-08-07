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

import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Base strategy for defining execution strategies that can support Stream-like 
 * sequential and parallel aggregate operation in the distributable environment. 
 * 
 * @param <T> the type of streamable elements of this {@link ExecutableDStream}
 */
public interface ExecutableDStream<T>{
	
	/**
	 * Will execute the task represented by this {@link ExecutableDStream} returning the result 
	 * as {@link Future} of {@link Stream} which represents the result of the entire execution.
	 * Such result itself consists of {@link Stream}s which represent the following:<br><br>
	 * 
	 * If this {@link ExecutableDStream} is represented by the {@link DStream}, 
	 * then each {@link Stream} contained within the common result corresponds to individual 
	 * result partition: <br>
	 * <pre>
	 * 	Stream - (handle to the entire result of the execution)
	 * 	   |___ Stream - (partition-1)
	 *	   |___ Stream - (partition-2)
	 *	   |___ Stream - (partition-3)
	 * 	</pre>	
	 * T - represents the result type<br>
	 * <br>
	 * This is an <i>terminal</i> operation.
	 * 
	 * @param executionName the name of this execution
	 * @return result as java {@link Future} consisting of {@link Stream}s representing each 
	 * result partition.
	 */
	Future<Stream<Stream<T>>> executeAs(String executionName);
	
	/**
	 * Returns the value to be used to identify the source of this stream.
	 * 
	 * @return value to be used to identify the source of this stream.
	 */
	String getSourceIdentifier();
}
