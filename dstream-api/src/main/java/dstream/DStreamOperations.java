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
 * Represents an <i>execution graph</i> built from the invocations on 
 * the {@link DStream}.<br>
 * <b><i>Execution graph</i></b> is a sort of an <i>abstract syntax tree</i> (AST)
 * of finalized {@link DStreamOperation}s ready to be mapped to and executed by the 
 * target system with minimal to no modifications. <br>
 *
 */
public final class DStreamOperations {

	private final List<DStreamOperation> operations;

	private final Class<?> sourceElementType;

	private final String executioniGraphName;
	
	/**
	 * 
	 */
	DStreamOperations(Class<?> sourceElementType, String executioniGraphName, List<DStreamOperation> operations){
		this.sourceElementType = sourceElementType;
		this.executioniGraphName = executioniGraphName;
		this.operations = operations;
	}
	
	/**
	 * Returns immutable {@link List} of {@link DStreamOperation}s.
	 */
	public List<DStreamOperation> getOperations() {
		return this.operations;
	}

	/**
	 * Returns the type of element of this execution graph. The type derives from the 
	 * type declared by the {@link DStream} during its initial construction.
	 */
	public Class<?> getSourceElementType() {
		return this.sourceElementType;
	}

	/**
	 * Returns the name of this execution graph. The name derives from the name
	 * given to the {@link DStream} during its initial construction.
	 */
	public String getName() {
		return this.executioniGraphName;
	}
	
	/**
	 * 
	 */
	@Override
	public String toString(){
		return "OPERATIONS: {" + this.operations.stream().map(s -> s.toString()).reduce("", (a,b) -> a.concat(b + "")) + "}";
	}
}
