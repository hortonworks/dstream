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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import dstream.function.AbstractMultiStreamProcessingFunction;
import dstream.function.BiFunctionToBinaryOperatorAdapter;
import dstream.function.DStreamToStreamAdapterFunction;
import dstream.function.KeyValueMappingFunction;
import dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.function.StreamJoinerFunction;
import dstream.function.StreamUnionFunction;
import dstream.function.ValuesAggregatingFunction;
import dstream.function.ValuesReducingFunction;
import dstream.support.Aggregators;

/**
 * 
 */
final class StreamOperationBuilder {

	private StreamOperation currentStreamOperation;
	
	private DStreamInvocationPipeline invocationPipeline;
	
	private Properties executionConfig;
	
	private int operationIdCounter;
	
	/**
	 */
	StreamOperationBuilder(DStreamInvocationPipeline invocationPipeline, Properties executionConfig){
		this.invocationPipeline = invocationPipeline;
		this.executionConfig = executionConfig;
	}
	
	/**
	 * 
	 */
	public StreamOperations build(){		
		for (DStreamInvocation invocation : this.invocationPipeline.getInvocations()) {
			this.addInvocation(invocation);
		}
		
		StreamOperation parent = this.currentStreamOperation;	
		List<StreamOperation> operationList = new ArrayList<>();
		if (parent != null){
			do {
				operationList.add(parent);
				parent = parent.getParent();
			} while (parent != null);
		}
		
		Collections.reverse(operationList);
				
		StreamOperations operations = new StreamOperations(
				this.invocationPipeline.getSourceElementType(), 
				this.invocationPipeline.getSourceIdentifier(), 
				this.invocationPipeline.getStreamType(), 
				Collections.unmodifiableList(operationList));
		
		return operations;
	}
	
	/**
	 * 
	 */
	private void addInvocation(DStreamInvocation invocation) {
		String operationName = invocation.getMethod().getName();
		if (this.isTransformation(operationName)){
			this.addTransformationOperation(invocation);
		}
		else if (this.isShuffle(operationName)){
			if (operationName.equals("join") || operationName.equals("union")){
				this.addStreamCombineOperation(invocation);
			}
			else {
				if (operationName.equals("reduceValues") || operationName.equals("aggregateValues")){
					this.addAggregationOperation(invocation);
				}
				else if (operationName.equals("group")){
					this.addGroupOperation(invocation);
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void addGroupOperation(DStreamInvocation invocation) {
		Object[] arguments = invocation.getArguments();

		SerFunction classifier = (SerFunction) arguments[0];
		
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++);
			this.currentStreamOperation.addStreamOperationFunction("map", s -> s);
		}
		this.currentStreamOperation.setGroupClassifier(classifier);
	}
	
	/**
	 * 
	 */
	private void addStreamCombineOperation(DStreamInvocation invocation){
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		String operationName = method.getName();
		
		DStreamInvocationPipeline dependentPipeline = (DStreamInvocationPipeline) arguments[0];
		StreamOperationBuilder dependentBuilder = new StreamOperationBuilder(dependentPipeline, this.executionConfig);
		StreamOperations dependentOperations = dependentBuilder.build();
		int joiningStreamsSize = dependentPipeline.getStreamType().getTypeParameters().length;
		
		if (this.currentStreamOperation == null){
			this.createStreamCombiningOperation(operationName);
		}
		else if (!"join".equals(this.currentStreamOperation.getLastOperationName()) && !"union".equals(this.currentStreamOperation.getLastOperationName())){
			this.createStreamCombiningOperation(operationName);
		}
		
		@SuppressWarnings("rawtypes")
		SerFunction f = this.currentStreamOperation.getStreamOperationFunction();
		AbstractMultiStreamProcessingFunction streamJoiner = (AbstractMultiStreamProcessingFunction) f;
		this.currentStreamOperation.addDependentStreamOperations(dependentOperations);
		streamJoiner.addCheckPoint(joiningStreamsSize);
		if (invocation.getSupplementaryOperation() != null){
			streamJoiner.addTransformationOrPredicate("filter", invocation.getSupplementaryOperation());
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private void createStreamCombiningOperation(String operationName){
		StreamOperation newStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		SerFunction streamCombiner = this.createStreamCombiner(operationName);
		newStreamOperation.addStreamOperationFunction(operationName, streamCombiner);
		this.currentStreamOperation = newStreamOperation;
	}
	
	/**
	 * 
	 */
	private AbstractMultiStreamProcessingFunction createStreamCombiner(String operationName) {
		return operationName.equals("join")
				? new StreamJoinerFunction()
					: new StreamUnionFunction(operationName.equals("union"));
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void addTransformationOperation(DStreamInvocation invocation){
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		String operationName = method.getName();
		
		SerFunction<Stream<?>, Stream<?>> currentStreamFunction = operationName.equals("compute") 
				? (SerFunction<Stream<?>, Stream<?>>) arguments[0]
						: new DStreamToStreamAdapterFunction(operationName, arguments[0]);
				
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++);
		}
		this.currentStreamOperation.addStreamOperationFunction(operationName, currentStreamFunction);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void addAggregationOperation(DStreamInvocation invocation) {
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		String operationName = method.getName();
		
		SerFunction keyMapper = (SerFunction) arguments[0];
		SerFunction valueMapper = (SerFunction) arguments[1];
		SerBinaryOperator valueAggregator = operationName.equals("reduceValues") 
				? (SerBinaryOperator)arguments[2]
						: new BiFunctionToBinaryOperatorAdapter(Aggregators::aggregateToList);
				
		int operationId = this.currentStreamOperation == null ? 1 : this.currentStreamOperation.getId();
		String propertyName = DStreamConstants.MAP_SIDE_COMBINE + operationId + "_" + this.invocationPipeline.getSourceIdentifier();
		boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));
		
		SerFunction kvMapper = new KeyValueMappingFunction<>(keyMapper, valueMapper, mapSideCombine ? valueAggregator : null);
		
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++);
		}
		this.currentStreamOperation.addStreamOperationFunction("mapKeyValues", kvMapper);
		
		StreamOperation newStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		SerFunction newStreamFunction = operationName.equals("reduceValues") 
				? new ValuesReducingFunction<>(valueAggregator)
						: new ValuesAggregatingFunction<>(valueAggregator);
		newStreamOperation.addStreamOperationFunction(operationName, newStreamFunction);
		this.currentStreamOperation = newStreamOperation;
	}
	
	/**
	 * 
	 */
	private boolean isTransformation(String operationName){
		return operationName.equals("flatMap") || 
			   operationName.equals("map") || 
			   operationName.equals("filter") ||
			   operationName.equals("compute");
	}
	
	/**
	 * 
	 */
	private boolean isShuffle(String operationName){
		return operationName.equals("reduceValues") ||
			   operationName.equals("aggregateValues") ||
			   operationName.equals("join") ||
			   operationName.equals("union") ||
			   operationName.equals("unionAll") ||
			   operationName.equals("group");
	}
}
