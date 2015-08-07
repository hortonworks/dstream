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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A sequence of {@link DStreamInvocation}s produced by the {@link DStreamInvocationPipelineBuilder}.
 */
public final class DStreamInvocationPipeline {

	private final List<DStreamInvocation> invocations;

	private final Class<?> sourceElementType;
	
	private final String sourceIdentifier;
	
	private final Class<?> streamType;
	
	/**
	 * 
	 * @param sourceElementType
	 * @param sourceIdentifier
	 * @param streamType
	 */
	protected DStreamInvocationPipeline(Class<?> sourceElementType, String sourceIdentifier, Class<?> streamType){
		this.sourceElementType = sourceElementType;
		this.sourceIdentifier = sourceIdentifier;
		this.invocations = new ArrayList<>();
		this.streamType = streamType;
	}
	
	/**
	 * 
	 * @return
	 */
	public Class<?> getStreamType() {
		return this.streamType;
	}

	/**
	 * 
	 * @return
	 */
	public List<DStreamInvocation> getInvocations() {
		return Collections.unmodifiableList(this.invocations);
	}

	/**
	 * 
	 * @return
	 */
	public Class<?> getSourceElementType() {
		return this.sourceElementType;
	}

	/**
	 * 
	 * @return
	 */
	public String getSourceIdentifier() {
		return this.sourceIdentifier;
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addInvocation(DStreamInvocation invocation){
		this.invocations.add(invocation);
	}
	
	/**
	 * 
	 * @param invocation
	 */
	protected void addAllInvocations(List<DStreamInvocation> invocations){
		this.invocations.addAll(invocations);
	}
}
