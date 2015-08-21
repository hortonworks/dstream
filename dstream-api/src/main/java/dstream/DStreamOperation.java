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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import dstream.function.AbstractMultiStreamProcessingFunction;
import dstream.function.KeyValueMappingFunction;
import dstream.function.SerializableFunctionConverters.SerFunction;

/**
 * Represents an assembled and final unit of work which should be used by a target 
 * execution environment to build a target specific execution (e.g., DAG in Tez or Spark).<br>
 * 
 * It consists of all attributes required to build a target specific execution. 
 * The two most important once are:<br>
 * <ol>
 * 
 * <li>
 *   The {@link Serializable} <i>lambda expression</i> provided by the user (see {@link #getStreamOperationFunction()}). 
 *    </li>
 * <li>
 *   Dependent operations which is a {@link List} of individual stream operations that in essence represents 
 *   another execution pipeline. Dependent operations can only be present if this operation is performing some
 *   type of streams combine operation (e.g., join, union, unionAll)
 *    </li>
 * </ol>
 */
public final class DStreamOperation {

	private final int id;
	
	private DStreamOperation parent;
	
	@SuppressWarnings("rawtypes")
	private SerFunction streamOperationFunction;

	private List<String> operationNames;
	
	private List<DStreamOperations> dependentStreamOperations;
	
	private boolean isMapPartition;

	/**
	 * 
	 * @param id
	 */
	DStreamOperation(int id){
		this(id, null);
	}
	
	/**
	 * 
	 * @param id
	 * @param parent
	 */
	DStreamOperation(int id, DStreamOperation parent) {
		this.parent = parent;
		this.operationNames = new ArrayList<>();
		this.id = id;
	}
	
	public boolean isMapPartition() {
		return isMapPartition;
	}
	
	/**
	 * Returns <i>true</i> if this operation's function is an
	 * instance of {@link AbstractMultiStreamProcessingFunction}
	 * @return
	 */
	public boolean isStreamsCombiner() {
		return this.streamOperationFunction instanceof AbstractMultiStreamProcessingFunction;
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
	public List<DStreamOperations> getDependentStreamOperations(){
		return dependentStreamOperations == null ? Collections.emptyList() : Collections.unmodifiableList(this.dependentStreamOperations);
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isClassify(){
		return this.operationNames.contains("classify");
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isShuffle(){
		if (this.operationNames.size() > 0){
			String operationName = this.operationNames.get(0);
			return this.isShuffle(operationName);
		}
		return false;
	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public SerFunction<Stream<?>, Stream<?>> getStreamOperationFunction() {
		return this.streamOperationFunction;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getLastOperationName() {
		return this.operationNames.size() > 0 
				? this.operationNames.get(this.operationNames.size()-1)
						: null;
	}
	
	/**
	 * 
	 * @param operationName
	 * @param function
	 */
	@SuppressWarnings("unchecked")
	void addStreamOperationFunction(String operationName, SerFunction<?,?> function){
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
	 */
	AbstractMultiStreamProcessingFunction getStreamsCombiner() {
		if (isStreamsCombiner()) {
			return (AbstractMultiStreamProcessingFunction) this.streamOperationFunction;
		}
		throw new IllegalStateException("THis operation is not streams combiner");
	}

	/**
	 * 
	 * @param operationName
	 * @param streamsCombiner
	 */
	void setStreamsCombiner(String operationName, AbstractMultiStreamProcessingFunction streamsCombiner) {
		this.operationNames.add(operationName);
		this.streamOperationFunction = streamsCombiner;
	}
	
	/**
	 * 
	 * @return
	 */
	DStreamOperation getParent(){
		return this.parent;
	}
	

	/**
	 * 
	 * @param dependentStreamOperations
	 */
	void addDependentStreamOperations(DStreamOperations dependentStreamOperations){
		if (this.dependentStreamOperations == null){
			this.dependentStreamOperations = new ArrayList<>();
		}
		this.dependentStreamOperations.add(dependentStreamOperations);
	}
	
	/**
	 * 
	 * @param isMapPartition
	 */
	void setMapPartition() {
		this.isMapPartition = true;
	}
	
	/**
	 * 
	 * @param operationName
	 * @return
	 */
	private boolean isShuffle(String operationName){
		return operationName.equals("reduceValues") ||
			   operationName.equals("aggregateValues") ||
			   operationName.equals("join") ||
			   operationName.equals("union") ||
			   operationName.equals("unionAll") ||
			   operationName.equals("classify") ||
			   operationName.equals("load");
	}
	
}
