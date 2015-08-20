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
 * Builder of DStreamOperations
 */
final class DStreamOperationsBuilder {

	private DStreamOperation currentStreamOperation;
	
	private DStreamInvocationChain invocationPipeline;
	
	private Properties executionConfig;
	
	private int operationIdCounter;
	
	private boolean streamsCombine;
	
	@SuppressWarnings("unchecked")
	private final SerFunction<Stream<Entry<Object, Object>>, Stream<Object>> unmapFunction = stream -> stream
			.flatMap(entry -> StreamSupport.stream(Spliterators.spliteratorUnknownSize((Iterator<Object>)entry.getValue(), Spliterator.ORDERED), false).sorted());

	
	/**
	 */
	DStreamOperationsBuilder(DStreamInvocationChain invocationPipeline, Properties executionConfig){
		this.invocationPipeline = invocationPipeline;
		this.executionConfig = executionConfig;
	}
	
	/**
	 * 
	 */
	protected DStreamOperations build(){	
		return this.doBuild(false);
	}
	
	/**
	 * The 'dependentStream' attribute implies that the DStreamOperations the methid is about to build 
	 * is a dependency of another DStreamOperations (e.g., for cases such as Join, union etc.)
	 * Basically, each process should have at least two operations - 'read -> write' with shuffle in between.
	 * For cases where we are building a dependent operations the second operation is actually 
	 * implicit (the operation that does the join, union etc.).
	 */
	@SuppressWarnings("rawtypes")
	private DStreamOperations doBuild(boolean isDependent){	
		this.invocationPipeline.getInvocations().forEach(this::addInvocation);
		
		if (this.requiresInitialSetOfOperations()){
			this.createDefaultExtractOperation();
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction(Ops.map.name(), s -> s);
		}
		else if (this.requiresPostShuffleOperation(isDependent)){
			SerFunction loadFunc = this.determineUnmapFunction(this.currentStreamOperation.getLastOperationName());
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction("load", loadFunc);
		}
		
		//
		DStreamOperation parent = this.currentStreamOperation;			
		List<DStreamOperation> operationList = new ArrayList<>();
		do {
			operationList.add(parent);
			parent = parent.getParent();
		} while (parent != null);	
		Collections.reverse(operationList);
		//	
		
		DStreamOperations operations = new DStreamOperations(
				this.invocationPipeline.getSourceElementType(), 
				this.invocationPipeline.getSourceIdentifier(), 
				Collections.unmodifiableList(operationList));
		
		return operations;
	}
	
	/**
	 * Will return <i>true</i> if this builder doesn't have any operations.
	 * Typical for cases where DStream is executed without any operations 
	 * invoked (e.g., DStream.ofType(String.class, "wc").executeAs("WordCount"); )
	 */
	private boolean requiresInitialSetOfOperations(){
		return this.currentStreamOperation == null;
	}
	
	/**
	 * Will return <i>true</i> if a post shuffle operation is required.
	 * Typical case is where this builder has only one operation and 
	 * it's building a non-dependent stream operations. 
	 */
	private boolean requiresPostShuffleOperation(boolean isDependent){
		return this.currentStreamOperation.getParent() == null && !isDependent;
	}
	
	/**
	 * Determines which unmap function to use. For cases when previous operation was 'classify'
	 * the unmap function is {@link #unmapFunction} otherwise it will produce a pass thru function.
	 */
	@SuppressWarnings("rawtypes")
	private SerFunction determineUnmapFunction(String lastOperationName){
		return lastOperationName.equals(Ops.classify.name()) ? this.unmapFunction : s -> s; 
	}
	
	/**
	 * Will create an operation named 'extract' with a pass-thru function
	 */
	private void createDefaultExtractOperation(){
		this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++);
		this.currentStreamOperation.addStreamOperationFunction(Ops.extract.name(), s -> s);
	}
	
	/**
	 * 
	 */
	private void addInvocation(DStreamInvocation invocation) {
		Ops operation = Ops.valueOf(invocation.getMethod().getName());
		if (Ops.isTransformation(operation)){
			this.addTransformationOperation(invocation);
		}
		else if (Ops.isShuffle(operation)){
			if (operation.equals(Ops.join) || operation.equals(Ops.union) ||operation.equals(Ops.unionAll)){
				this.streamsCombine = true;
				this.addStreamsCombineOperation(invocation);
			}
			else {
				this.streamsCombine = false;
				
				if (operation.equals(Ops.reduceValues) || operation.equals(Ops.aggregateValues)){
					this.addAggregationOperation(invocation);
				}
				else if (operation.equals(Ops.classify)){
					this.addClassifyOperation(invocation);
				}
			}
		}
		else if (Ops.isStreamTerminal(operation)){
			this.addStreamTerminalOperation(invocation);
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void addClassifyOperation(DStreamInvocation invocation) {
		Object[] arguments = invocation.getArguments();
		SerFunction classifier = (SerFunction) arguments[0];
		SerFunction kvMapper = new KeyValueMappingFunction<>(classifier, s -> s, null);
		
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		}
		
		this.currentStreamOperation.addStreamOperationFunction(Ops.classify.name(), kvMapper);
		if (this.currentStreamOperation.isClassify() && this.currentStreamOperation.getParent() != null){
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction(Ops.load.name(), this.unmapFunction);
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
		Ops operation = Ops.valueOf(method.getName());
		
		AbstractMultiStreamProcessingFunction streamsCombiner;
		if (this.currentStreamOperation == null){
			this.createDefaultExtractOperation();
			// this condition possible when doing join without classification, so no need to 
			streamsCombiner = this.createStreamCombiner(operation.name(), s -> s);
			DStreamOperation newStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);

			this.currentStreamOperation = newStreamOperation;
			this.currentStreamOperation.setStreamsCombiner(operation.name(), streamsCombiner);
		}
		else if (this.currentStreamOperation.isStreamsCombiner()) {
			streamsCombiner = this.currentStreamOperation.getStreamsCombiner();
		}
		else {
			streamsCombiner = this.createStreamCombiner(operation.name(), this.determineUnmapFunction(this.currentStreamOperation.getLastOperationName()));
			DStreamOperation newStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);

			this.currentStreamOperation = newStreamOperation;
			this.currentStreamOperation.setStreamsCombiner(operation.name(), streamsCombiner);
		}
		
		DStreamInvocationChain dependentPipeline = (DStreamInvocationChain) arguments[0];
		DStreamOperationsBuilder dependentBuilder = new DStreamOperationsBuilder(dependentPipeline, this.executionConfig);
		DStreamOperations dependentOperations = dependentBuilder.doBuild(true);
		int joiningStreamsSize = dependentPipeline.getStreamType().getTypeParameters().length;
		
		this.currentStreamOperation.addDependentStreamOperations(dependentOperations); // should it be set instead of add?
		streamsCombiner.addCheckPoint(joiningStreamsSize);
		if (invocation.getSupplementaryOperation() != null){
			streamsCombiner.addTransformationOrPredicate(Ops.filter.name(), invocation.getSupplementaryOperation());
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({"unchecked", "rawtypes" })
	private AbstractMultiStreamProcessingFunction createStreamCombiner(String operationName, SerFunction firstStreamPreProcessingFunction) {
		return operationName.equals(Ops.join.name())
				? new StreamJoinerFunction(firstStreamPreProcessingFunction)
					: new StreamUnionFunction(operationName.equals(Ops.union.name()), firstStreamPreProcessingFunction);
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void addTransformationOperation(DStreamInvocation invocation){
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		Ops operation = Ops.valueOf(method.getName());
		
		SerFunction<Stream<?>, Stream<?>> currentStreamFunction = operation.equals(Ops.compute) 
				? (SerFunction<Stream<?>, Stream<?>>) arguments[0]
						: new DStreamToStreamAdapterFunction(operation.name(), arguments.length > 0 ? arguments[0] : null);
				
		if (this.streamsCombine){
			@SuppressWarnings("rawtypes")
			SerFunction currentFunction = this.currentStreamOperation.getStreamOperationFunction();
			AbstractMultiStreamProcessingFunction joiner = (AbstractMultiStreamProcessingFunction) currentFunction;
			joiner.addTransformationOrPredicate(currentStreamFunction);
		}
		else {
			if (this.currentStreamOperation == null){
				this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++);
			} 
			else if (this.currentStreamOperation.getLastOperationName().equals(Ops.classify.name())){
				this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
				this.currentStreamOperation.addStreamOperationFunction(operation.name(), this.unmapFunction);
			}
			this.currentStreamOperation.addStreamOperationFunction(operation.name(), currentStreamFunction);
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void addAggregationOperation(DStreamInvocation invocation) {
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		Ops operation = Ops.valueOf(method.getName());
		
		SerFunction keyMapper = (SerFunction) arguments[0];
		SerFunction valueMapper = (SerFunction) arguments[1];
		SerBinaryOperator valueAggregator = operation.equals(Ops.reduceValues) 
				? (SerBinaryOperator)arguments[2]
						: new BiFunctionToBinaryOperatorAdapter(Aggregators::aggregateToList);
				
		int operationId = this.currentStreamOperation == null ? 1 : this.currentStreamOperation.getId();
		String propertyName = DStreamConstants.MAP_SIDE_COMBINE + operationId + "_" + this.invocationPipeline.getSourceIdentifier();
		boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));
		
		SerFunction kvMapper = new KeyValueMappingFunction<>(keyMapper, valueMapper, mapSideCombine ? valueAggregator : null);
		
		this.adjustCurrentStreamState();
		
		this.currentStreamOperation.addStreamOperationFunction(Ops.mapKeyValues.name(), kvMapper);

		DStreamOperation newStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		SerFunction newStreamFunction = operation.equals(Ops.reduceValues) 
				? new ValuesReducingFunction<>(valueAggregator)
						: new ValuesAggregatingFunction<>(valueAggregator);
		newStreamOperation.addStreamOperationFunction(operation.name(), newStreamFunction);
		this.currentStreamOperation = newStreamOperation;
	}
	
	/**
	 * 
	 */
	private void addStreamTerminalOperation(DStreamInvocation invocation){
		if (invocation.getMethod().getName().equals(Ops.count.name())){
			SerFunction<Object, Integer> keyMapper = s -> 0;
			SerFunction<Object, Long> valueMapper = s -> 1L;
			SerBinaryOperator<Long> valueAggregator = Long::sum;
			SerFunction<Stream<Object>, Stream<Entry<Integer, Long>>> kvMapper = 
					new KeyValueMappingFunction<Object, Integer, Long>(keyMapper, valueMapper, valueAggregator);
			
			this.adjustCurrentStreamState();
			
			this.currentStreamOperation.addStreamOperationFunction(Ops.mapKeyValues.name(), kvMapper);

			SerFunction<Stream<Entry<Integer,Iterator<Long>>>,Stream<Long>> valueReducingFunction = 
					new ValuesReducingFunction<Integer, Long, Long>(valueAggregator);
			DStreamOperation valueReducingOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
				
			valueReducingOperation.addStreamOperationFunction(Ops.reduceValues.name(), valueReducingFunction);
			this.currentStreamOperation = valueReducingOperation;
			
			DStreamOperation mappingOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			mappingOperation.addStreamOperationFunction(Ops.map.name(), this.unmapFunction);
			this.currentStreamOperation = mappingOperation;
		}
	}
	
	/**
	 * 
	 */
	private void adjustCurrentStreamState(){
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++);
		}
		
		if (this.currentStreamOperation.isClassify()){
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction(Ops.load.name(), this.unmapFunction);
		}
	}
}
