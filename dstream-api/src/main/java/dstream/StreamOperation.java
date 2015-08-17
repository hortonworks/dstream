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
import java.util.stream.Stream;

import dstream.function.AbstractMultiStreamProcessingFunction;
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
	
//	private AbstractMultiStreamProcessingFunction streamsCombiner;
	
	private boolean streamsCombiner;

	

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
	
	AbstractMultiStreamProcessingFunction getStreamsCombiner() {
		if (isStreamsCombiner()) {
			return (AbstractMultiStreamProcessingFunction) this.streamOperationFunction;
		}
		throw new IllegalStateException("THis operation is not streams combiner");
	}

	void setStreamsCombiner(AbstractMultiStreamProcessingFunction streamsCombiner) {
		this.streamOperationFunction = streamsCombiner;
		this.streamsCombiner = true;
	}
	
	public boolean isStreamsCombiner() {
		return streamsCombiner;
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
		return dependentStreamOperations == null ? Collections.emptyList() : Collections.unmodifiableList(this.dependentStreamOperations);
	}
	
//	public boolean isShuffle(){
//		if (this.operationNames.size() > 0){
//			String operationName = this.operationNames.get(0);
//			return this.isShuffle(operationName);
//		}
//		return false;
//	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public SerFunction<Stream<?>, Stream<?>> getStreamOperationFunction() {
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
	
//	private boolean isShuffle(String operationName){
//		return operationName.equals("reduceValues") ||
//			   operationName.equals("aggregateValues") ||
//			   operationName.equals("join") ||
//			   operationName.equals("union") ||
//			   operationName.equals("unionAll") ||
//			   operationName.equals("group");
//	}
}
