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

import dstream.function.KeyValueMappingFunction;
import dstream.function.SerializableFunctionConverters.SerFunction;

/**
 * 
 */
public class StreamOperation {

	private StreamOperation parent;
	
	@SuppressWarnings("rawtypes")
	private SerFunction streamOperationFunction;

	private List<String> operationNames;
	
	private final int id;
	
	private List<StreamOperations> dependentStreamOperations;
	
	private String lastOperationName;
	
	private SerFunction<?, ?> groupClassifier;

	/**
	 * 
	 * @param id
	 */
	StreamOperation(int id){
		this(id, null);
	}
	
	/**
	 * 
	 * @param id
	 * @param parent
	 */
	StreamOperation(int id, StreamOperation parent) {
		this.parent = parent;
		this.operationNames = new ArrayList<>();
		this.id = id;
	}
	
	/**
	 * 
	 * @return
	 */
	public int getId(){
		return this.id;
	}
	
	/**
	 * 
	 */
	public String toString(){
		return this.operationNames.toString();
	}
	
	/**
	 * 
	 * @return
	 */
	public List<StreamOperations> getDependentStreamOperations(){
		return Collections.unmodifiableList(this.dependentStreamOperations);
	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public SerFunction getStreamOperationFunction() {
		return streamOperationFunction;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getLastOperationName() {
		return lastOperationName;
	}
	
	/**
	 * 
	 * @return
	 */
	public SerFunction<?, ?> getGroupClassifier() {
		return groupClassifier;
	}

	/**
	 * 
	 * @param groupClassifier
	 */
	void setGroupClassifier(SerFunction<?, ?> groupClassifier) {
		this.groupClassifier = groupClassifier;
	}
	
	/**
	 * 
	 * @param operationName
	 * @param function
	 */
	@SuppressWarnings("unchecked")
	void addStreamOperationFunction(String operationName, SerFunction<?,?> function){
		this.lastOperationName = operationName;
		if (this.streamOperationFunction != null){
			this.streamOperationFunction = this.streamOperationFunction.andThen(function);
		}
		else {
			this.streamOperationFunction = function;
		}
		this.operationNames.add(operationName);
		if (function instanceof KeyValueMappingFunction){
			if ( ((KeyValueMappingFunction<?,?,?>)function).aggregatesValues() ) {
				String lastOperationName = this.operationNames.get(this.operationNames.size()-1);
				lastOperationName = lastOperationName + "{reducingValues}";  
				this.operationNames.set(this.operationNames.size()-1, lastOperationName);
			}
		}
	}
	
	/**
	 * 
	 * @return
	 */
	StreamOperation getParent(){
		return this.parent;
	}
	

	/**
	 * 
	 * @param dependentStreamOperations
	 */
	void addDependentStreamOperations(StreamOperations dependentStreamOperations){
		if (this.dependentStreamOperations == null){
			this.dependentStreamOperations = new ArrayList<>();
		}
		this.dependentStreamOperations.add(dependentStreamOperations);
	}
}
