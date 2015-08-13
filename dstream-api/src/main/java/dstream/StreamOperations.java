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

import java.util.List;

public final class StreamOperations {

	private final List<StreamOperation> operations;

	private final Class<?> sourceElementType;

	private final String pipelineName;
	
	private final Class<?> streamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param pipelineName
	 * @param streamType
	 * @param operations
	 */
	StreamOperations(Class<?> sourceElementType, String pipelineName, Class<?> streamType, List<StreamOperation> operations){
		this.sourceElementType = sourceElementType;
		this.pipelineName = pipelineName;
		this.operations = operations;
		this.streamType = streamType;
	}
	
	/**
	 * Returns immutable {@link List} of {@link StreamOperation}s.
	 * @return
	 */
	public List<StreamOperation> getOperations() {
		return operations;
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getSourceElementType() {
		return sourceElementType;
	}

	/**
	 * 
	 * @return
	 */
	public String getPipelineName() {
		return this.pipelineName;
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getStreamType() {
		return streamType;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return "OPERATIONS: {" + this.operations.stream().map(s -> s.toString()).reduce("", (a,b) -> a.concat(b + "")) + "}";
	}
}
