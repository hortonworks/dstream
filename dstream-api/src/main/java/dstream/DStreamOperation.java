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

import dstream.SerializableStreamAssets.SerFunction;
import dstream.function.KeyValueMappingFunction;

/**
 * Represents an assembled and final unit of work (i.e., execution stage) to be 
 * used by a target execution environment when building target specific 
 * execution (e.g., DAG in Tez or Spark).<br>
 * 
 * It consists of all attributes required to build a target specific execution. 
 * The two most important once are:<br>
 * <ol>
 * <li>
 *   The {@link Serializable} <i>lambda expression</i> which includes the lambda 
 *   expression provided by the user (see {@link #getStreamOperationFunction()}). <br>
 *   <i>NOTE: While preserving end user's intentions, the final lambda
 *   may be the result of a composition with other implicit or explicit lambdas 
 *   during optimization phase.</i>
 *    </li>
 * <li>
 *   Dependent operations which is a {@link List} of individual {@link DStreamExecutionGraph} 
 *   that in essence represents another execution pipeline. Dependent operations 
 *   can only be present if this operation is performing some
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
	
	private List<DStreamExecutionGraph> combinableExecutionGraphs;
	
	/**
	 * Constructs this {@link DStreamOperation} with the given <i>id</i>.
	 */
	DStreamOperation(int id){
		this(id, null);
	}
	
	/**
	 *  Constructs this {@link DStreamOperation} with the given <i>id</i> and parent 
	 *  operation.
	 */
	DStreamOperation(int id, DStreamOperation parent) {
		this.parent = parent;
		this.operationNames = new ArrayList<>();
		this.id = id;
	}
	
	/**
	 * Returns <i>true</i> if this operation's function is an
	 * instance of {@link AbstractMultiStreamProcessingFunction}
	 */
	public boolean isStreamsCombiner() {
		return this.streamOperationFunction instanceof AbstractMultiStreamProcessingFunction;
	}
	
	/**
	 * Returns the <i>id</i> of this {@link DStreamOperation}. 
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
	 * Returns the {@link List} of combinable {@link DStreamExecutionGraph}
	 * where each {@link DStreamExecutionGraph} implies some type of 
	 * combine functionality with current operation (i.e., join, union, unionAll etc.)
	 */
	public List<DStreamExecutionGraph> getCombinableExecutionGraphs(){
		return this.combinableExecutionGraphs == null 
				? Collections.emptyList() 
						: Collections.unmodifiableList(this.combinableExecutionGraphs);
	}
	
	/**
	 * Returns <i>true</i> if the last operation which composes this {@link DStreamOperation}
	 * if {@link Ops#classify}
	 */
	public boolean isClassify(){
		return Ops.classify.name().equals(this.getLastOperationName());
	}
	
	/**
	 * Returns <i>true</i> if this {@link DStreamOperation} represents a <i>shuffle</i>
	 * operation (see {@link Ops#isShuffle(String)}
	 */
	public boolean isShuffle(){
		if (this.operationNames.size() > 0){
			String operationName = this.operationNames.get(0);
			return Ops.isShuffle(operationName);
		}
		return false;
	}
	
	/**
	 * Returns a {@link SerFunction} to be applied on the localized {@link Stream} of data
	 * processed by a target task.
	 * It includes the lambda expression provided by the end user.<br>
     * <i>NOTE: While preserving end user's intentions, the final function
     * may be the result of a composition with other implicit or explicit functions 
     * during optimization phase (see {@link #addStreamOperationFunction(String, SerFunction)}).</i>
	 */
	@SuppressWarnings("unchecked")
	public SerFunction<Stream<?>, Stream<?>> getStreamOperationFunction() {
		return this.streamOperationFunction;
	}
	
	/**
	 * Returns the last operation which composes this {@link DStreamOperation}.<br>
	 * For example:
	 * <pre>
	 * DStream.ofType(String.class, "wc")
	 *     .flatMap(..)
	 *     .map(..)
	 *     .filter(..)
	 *     . . .
	 * </pre>
	 * In the above, all three operations are <i>composable-transformations</i>
	 * and will be composed into a single {@link DStreamOperation} with the list of 
	 * operation names - <i>[flatMap, map, filter]</i>. In the given scenario this method
	 * will return 'filter' since it is the last operation name composing this 
	 * {@link DStreamOperation}.
	 */
	public String getLastOperationName() {
		return this.operationNames.size() > 0 
				? this.operationNames.get(this.operationNames.size()-1)
						: null;
	}
	
	/**
	 * Will add the given {@link SerFunction} to this {@link DStreamOperation} by 
	 * composing it with the previous function. If previous function is <i>null</i>
	 * the given function becomes the root function of this operation.<br>
	 * It also adds the given <i>operationName</i> to the list of operation names
	 * which composes this {@link DStreamOperation}.<br>
	 * The final (composed) function represents the function to applied on the 
	 * localized {@link Stream} of data processed by a target task.
	 */
	@SuppressWarnings("unchecked")
	void addStreamOperationFunction(String operationName, SerFunction<?,?> function){
		this.operationNames.add(operationName);
		this.streamOperationFunction = this.streamOperationFunction != null
				? this.streamOperationFunction.andThen(function)
						: function;
		
		if (function instanceof KeyValueMappingFunction){
			if ( ((KeyValueMappingFunction<?,?,?>)function).aggregatesValues() ) {
				String lastOperationName = this.operationNames.get(this.operationNames.size()-1);
				lastOperationName = lastOperationName + "{reducingValues}";  
				this.operationNames.set(this.operationNames.size()-1, lastOperationName);
			}
		}
	}
	
	/**
	 * Sets the given instance of {@link AbstractMultiStreamProcessingFunction} as the 
	 * function of this {@link DStreamOperation}.
	 */
	void setStreamsCombiner(String operationName, AbstractMultiStreamProcessingFunction streamsCombiner) {
		this.operationNames.add(operationName);
		this.streamOperationFunction = streamsCombiner;
	}
	
	/**
	 * Returns {@link DStreamOperation} which is a parent to this operation.
	 */
	DStreamOperation getParent(){
		return this.parent;
	}

	/**
	 * Ads the given {@link DStreamExecutionGraph} to the {@link List} of combinable {@link DStreamExecutionGraph}.
	 * Each added {@link DStreamExecutionGraph} implies some type of 
	 * combine functionality with current operation (i.e., join, union, unionAll etc.)
	 */
	void addCombinableExecutionGraph(DStreamExecutionGraph combinableExecutionGraph){
		if (this.combinableExecutionGraphs == null){
			this.combinableExecutionGraphs = new ArrayList<>();
		}
		this.combinableExecutionGraphs.add(combinableExecutionGraph);
	}
}
