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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import dstream.utils.Assert;

/**
 * 
 */
final class StreamOperationBuilder {

	private StreamOperation currentStreamOperation;
	
	private DStreamInvocationPipeline invocationPipeline;
	
	private Properties executionConfig;
	
	private int operationIdCounter;
	
	private boolean inMulti;
	
	@SuppressWarnings("unchecked")
	private final SerFunction<Stream<Entry<Object, Object>>, Stream<Object>> unmapFunction = stream -> stream
			.flatMap(entry -> StreamSupport.stream(Spliterators.spliteratorUnknownSize((Iterator<Object>)entry.getValue(), Spliterator.ORDERED), false).sorted());

	
	/**
	 */
	StreamOperationBuilder(DStreamInvocationPipeline invocationPipeline, Properties executionConfig){
		this.invocationPipeline = invocationPipeline;
		this.executionConfig = executionConfig;
	}
	
	protected StreamOperations build(){	
		return this.doBuild(false);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private StreamOperations doBuild(boolean dependent){	
		for (DStreamInvocation invocation : this.invocationPipeline.getInvocations()) {
			this.addInvocation(invocation);
		}
		
		if (this.currentStreamOperation == null){
			this.createInitialStreamOperation();
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction("transform", s -> s);
		}
		else if (this.currentStreamOperation.getParent() == null && !dependent ){
			SerFunction loadFunc = this.determineUnmapFunction(this.currentStreamOperation.getLastOperationName());
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction("load", loadFunc);
		}
		
		StreamOperation parent = this.currentStreamOperation;	
		
		List<StreamOperation> operationList = new ArrayList<>();
		do {
			operationList.add(parent);
			parent = parent.getParent();
		} while (parent != null);
		
		Collections.reverse(operationList);
				
		StreamOperations operations = new StreamOperations(
				this.invocationPipeline.getSourceElementType(), 
				this.invocationPipeline.getSourceIdentifier(), 
				this.invocationPipeline.getStreamType(), 
				Collections.unmodifiableList(operationList));
		
		return operations;
	}
	
	@SuppressWarnings("rawtypes")
	private SerFunction determineUnmapFunction(String lastOperationName){
		return lastOperationName.equals("classify") ? this.unmapFunction : s -> s; 
	}
	
	/**
	 * 
	 */
	private void createInitialStreamOperation(){
		this.currentStreamOperation = new StreamOperation(this.operationIdCounter++);
		this.currentStreamOperation.addStreamOperationFunction("extract", s -> s);
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
			if (operationName.equals("join") || operationName.startsWith("union")){
				this.inMulti = true;
				this.addStreamsCombineOperation(invocation);
			}
			else {
				if (this.inMulti) {
					this.inMulti = false;
				}
				
				if (operationName.equals("reduceValues") || operationName.equals("aggregateValues")){
					this.addAggregationOperation(invocation);
				}
				else if (operationName.equals("classify")){
					this.addClassifyOperation(invocation);
				}
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void addClassifyOperation(DStreamInvocation invocation) {
		Object[] arguments = invocation.getArguments();
		SerFunction classifier = (SerFunction) arguments[0];
		SerFunction kvMapper = new KeyValueMappingFunction<>(classifier, s -> s, null);
		
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		}
		
		this.currentStreamOperation.addStreamOperationFunction("classify", kvMapper);
		if (this.currentStreamOperation.isClassify() && this.currentStreamOperation.getParent() != null){
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction("load", this.unmapFunction);
		}
	}
	
	/**
	 * 
	 */
	private void addStreamsCombineOperation(DStreamInvocation invocation){
		if (this.executionConfig.containsKey(DStreamConstants.PARALLELISM)){
			int parallelism = Integer.parseInt(this.executionConfig.getProperty(DStreamConstants.PARALLELISM));
			if (this.currentStreamOperation == null || 
				(this.currentStreamOperation.getParent() != null && !this.currentStreamOperation.getParent().isClassify()) ||
				(this.currentStreamOperation.getParent() == null && !this.currentStreamOperation.isClassify())){
				Assert.isTrue(parallelism == 1, "Combining streams without prior classification is not supported when parallelism is > 0");
			}
		}
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		String operationName = method.getName();
		
		AbstractMultiStreamProcessingFunction streamsCombiner;
		if (this.currentStreamOperation == null){
			this.createInitialStreamOperation();
			// this condition possible when doing join without classification, so no need to 
			streamsCombiner = this.createStreamCombiner(operationName, s -> s);
			StreamOperation newStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);

			this.currentStreamOperation = newStreamOperation;
			this.currentStreamOperation.setStreamsCombiner(operationName, streamsCombiner);
		}
		else if (this.currentStreamOperation.isStreamsCombiner()) {
			streamsCombiner = this.currentStreamOperation.getStreamsCombiner();
		}
		else {
			streamsCombiner = this.createStreamCombiner(operationName, this.determineUnmapFunction(this.currentStreamOperation.getLastOperationName()));
			StreamOperation newStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);

			this.currentStreamOperation = newStreamOperation;
			this.currentStreamOperation.setStreamsCombiner(operationName, streamsCombiner);
		}
		
		DStreamInvocationPipeline dependentPipeline = (DStreamInvocationPipeline) arguments[0];
		StreamOperationBuilder dependentBuilder = new StreamOperationBuilder(dependentPipeline, this.executionConfig);
		StreamOperations dependentOperations = dependentBuilder.doBuild(true);
		int joiningStreamsSize = dependentPipeline.getStreamType().getTypeParameters().length;
		
		this.currentStreamOperation.addDependentStreamOperations(dependentOperations); // should it be set instead of add?
		streamsCombiner.addCheckPoint(joiningStreamsSize);
		if (invocation.getSupplementaryOperation() != null){
			streamsCombiner.addTransformationOrPredicate("filter", invocation.getSupplementaryOperation());
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({"unchecked", "rawtypes" })
	private AbstractMultiStreamProcessingFunction createStreamCombiner(String operationName, SerFunction firstStreamPreProcessingFunction) {
		return operationName.equals("join")
				? new StreamJoinerFunction(firstStreamPreProcessingFunction)
					: new StreamUnionFunction(operationName.equals("union"), firstStreamPreProcessingFunction);
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
				
		if (this.inMulti){
			@SuppressWarnings("rawtypes")
			SerFunction currentFunction = this.currentStreamOperation.getStreamOperationFunction();
			AbstractMultiStreamProcessingFunction joiner = (AbstractMultiStreamProcessingFunction) currentFunction;
			joiner.addTransformationOrPredicate(currentStreamFunction);
		}
		else {
			if (this.currentStreamOperation == null){
				this.currentStreamOperation = new StreamOperation(this.operationIdCounter++);
			} 
			else if (this.currentStreamOperation.getLastOperationName().equals("classify")){
				this.currentStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
				this.currentStreamOperation.addStreamOperationFunction(operationName, this.unmapFunction);
			}
			this.currentStreamOperation.addStreamOperationFunction(operationName, currentStreamFunction);
		}
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
		
		if (this.currentStreamOperation.isClassify()){
			this.currentStreamOperation = new StreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			SerFunction loadFunc = this.determineUnmapFunction("classify");
			this.currentStreamOperation.addStreamOperationFunction("load", loadFunc);
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
			   operationName.equals("classify");
	}
}
