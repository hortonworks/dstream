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

/**
 * Represents the <i>execution pipeline</i> built from the invocations on the {@link DStream}.<br>
 * <b><i>Execution pipeline</i></b> is a sort of an abstract syntax tree (AST) containing finalized 
 * operations ready to be executed by the target system with minimal to no modifications. <br>
 * Individual operations are represented by a {@link DStreamOperation}.
 *
 */
public final class DStreamOperations {

	private final List<DStreamOperation> operations;

	private final Class<?> sourceElementType;

	private final String pipelineName;
	
	/**
	 * 
	 */
	DStreamOperations(Class<?> sourceElementType, String pipelineName, List<DStreamOperation> operations){
		this.sourceElementType = sourceElementType;
		this.pipelineName = pipelineName;
		this.operations = operations;
	}
	
	/**
	 * Returns immutable {@link List} of {@link DStreamOperation}s.
	 * @return
	 */
	public List<DStreamOperation> getOperations() {
		return this.operations;
	}

	/**
	 * Returns the type of the element the underlying pipeline was constructed with.
	 */
	public Class<?> getSourceElementType() {
		return this.sourceElementType;
	}

	/**
	 * Returns the name used during the construction of the {@link DStream} from which this 
	 * execution pipeline was built.
	 */
	public String getName() {
		return this.pipelineName;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return "OPERATIONS: {" + this.operations.stream().map(s -> s.toString()).reduce("", (a,b) -> a.concat(b + "")) + "}";
	}
}
