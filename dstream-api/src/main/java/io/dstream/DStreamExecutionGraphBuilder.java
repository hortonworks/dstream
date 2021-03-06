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
package io.dstream;

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

import io.dstream.DStreamInvocationChain.DStreamInvocation;
import io.dstream.SerializableStreamAssets.SerBinaryOperator;
import io.dstream.SerializableStreamAssets.SerComparator;
import io.dstream.SerializableStreamAssets.SerConsumer;
import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.function.BiFunctionToBinaryOperatorAdapter;
import io.dstream.function.DStreamToStreamAdapterFunction;
import io.dstream.function.KeyValueMappingFunction;
import io.dstream.function.ValuesAggregatingFunction;
import io.dstream.function.ValuesReducingFunction;
import io.dstream.support.Aggregators;
import io.dstream.utils.Assert;

/**
 * Builder of DStreamOperations
 */
final class DStreamExecutionGraphBuilder {
	
	private final DStreamInvocationChain invocationPipeline;
	
	private final Properties executionConfig;
	
	private final SerFunction<Stream<Entry<Object, Object>>, Stream<Object>> shuffleResultNormalizer;
	

	private DStreamOperation currentStreamOperation;
	
	private int operationIdCounter;
	
	private boolean combiningStreams;
	
	
	/**
	 */
	@SuppressWarnings("unchecked")
	DStreamExecutionGraphBuilder(DStreamInvocationChain invocationPipeline, Properties executionConfig){
		this.invocationPipeline = invocationPipeline;
		this.executionConfig = executionConfig;
		this.shuffleResultNormalizer = stream -> stream
				.flatMap(entry -> StreamSupport
						.stream(Spliterators.spliteratorUnknownSize((Iterator<Object>)entry.getValue(), Spliterator.ORDERED), false));
	}
	
	/**
	 * 
	 */
	protected DStreamExecutionGraph build(){	
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
	private DStreamExecutionGraph doBuild(boolean isDependent){	
		this.invocationPipeline.getInvocations().forEach(this::addInvocation);
		
		if (this.requiresInitialSetOfOperations()){
			this.createDefaultExtractOperation();
		}
		if (this.requiresPostShuffleOperation(isDependent)){
			SerFunction loadFunc = this.determineUnmapFunction(this.currentStreamOperation.getLastOperationName());
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			this.currentStreamOperation.addStreamOperationFunction(Ops.load.name(), loadFunc);
		}
		
		DStreamOperation parent = this.currentStreamOperation;			
		List<DStreamOperation> operationList = new ArrayList<>();
		do {
			operationList.add(parent);
			parent = parent.getParent();
		} while (parent != null);	
		Collections.reverse(operationList);
		//	
		
		DStreamExecutionGraph operations = new DStreamExecutionGraph(
				this.invocationPipeline.getSourceElementType(), 
				this.invocationPipeline.getSourceIdentifier(), 
				Collections.unmodifiableList(operationList));
		
		return operations;
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
				this.addStreamsCombineOperation(invocation);
				this.combiningStreams = true;
			}
			else {
				this.combiningStreams = false;			
				if (operation.equals(Ops.reduceValues) || operation.equals(Ops.aggregateValues)){
					this.addAggregationOperation(invocation);
				}
				else if (operation.equals(Ops.classify)){
					this.addClassifyOperation(invocation);
				}
			}
		}
		else if (Ops.isStreamReduce(operation)){
			this.addStreamReduceOperation(invocation);
		}
		else if (Ops.isStreamComparator(operation)){
			this.addStreamComparatorOperation(invocation);
		}
		else if (Ops.isStreamConsumer(operation)){
			this.addStreamConsumerOperation(invocation);
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

		if (this.currentStreamOperation.getParent() != null){
			this.addPostShuffleNormalizer();
		}
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private void addStreamsCombineOperation(DStreamInvocation invocation){
		if (this.executionConfig.containsKey(DStreamConstants.PARALLELISM)){
			int parallelism = Integer.parseInt(this.executionConfig.getProperty(DStreamConstants.PARALLELISM));
			if (this.currentStreamOperation == null || 
				(this.currentStreamOperation.getParent() != null && !this.currentStreamOperation.getParent().isClassify()) ||
				(this.currentStreamOperation.getParent() == null && !this.currentStreamOperation.isClassify())){
				Assert.isTrue(parallelism == 1, "Combining streams without prior classification is not supported when parallelism is > 1");
			}
		}
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		Ops operation = Ops.valueOf(method.getName());
		
		AbstractStreamMergingFunction streamsCombiner;
		if (this.currentStreamOperation == null){
			this.createDefaultExtractOperation();
			// this condition possible when doing join without classification, so no need to 
			streamsCombiner = this.createStreamCombiner(operation.name(), s -> s);
			DStreamOperation newStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);

			this.currentStreamOperation = newStreamOperation;
			this.currentStreamOperation.setStreamsCombiner(operation.name(), streamsCombiner);
		}
		else if (this.currentStreamOperation.isStreamsCombiner()) {
			streamsCombiner = (AbstractStreamMergingFunction)(SerFunction)this.currentStreamOperation.getStreamOperationFunction();
		}
		else {
			streamsCombiner = this.createStreamCombiner(operation.name(), this.determineUnmapFunction(this.currentStreamOperation.getLastOperationName()));
			DStreamOperation newStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);

			this.currentStreamOperation = newStreamOperation;
			this.currentStreamOperation.setStreamsCombiner(operation.name(), streamsCombiner);
		}
		
		DStreamInvocationChain dependentPipeline = (DStreamInvocationChain) arguments[0];
		DStreamExecutionGraphBuilder dependentBuilder = new DStreamExecutionGraphBuilder(dependentPipeline, this.executionConfig);
		DStreamExecutionGraph dependentOperations = dependentBuilder.doBuild(true);
		int joiningStreamsSize = dependentPipeline.getStreamType().getTypeParameters().length;
		
		this.currentStreamOperation.addCombinableExecutionGraph(dependentOperations); 
		streamsCombiner.addCheckPoint(joiningStreamsSize);
		if (invocation.getSupplementaryOperation() != null){
			streamsCombiner.addTransformationOrPredicate(Ops.filter.name(), invocation.getSupplementaryOperation());
		}
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
		
		if (this.combiningStreams){
			SerFunction<?,?> currentFunction = this.currentStreamOperation.getStreamOperationFunction();
			AbstractStreamMergingFunction joiner = (AbstractStreamMergingFunction) currentFunction;
			joiner.addTransformationOrPredicate(currentStreamFunction);
		}
		else {
			if (this.currentStreamOperation == null){
				this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++);
			} 
			else if (this.currentStreamOperation.getLastOperationName().equals(Ops.classify.name())){
				this.addPostShuffleNormalizer();
			}
				
			this.currentStreamOperation.addStreamOperationFunction(operation.name(), currentStreamFunction);
		}
	}
	
	/**
	 * 
	 */
	private void addAggregationOperation(DStreamInvocation invocation) {
		Method method = invocation.getMethod();
		Object[] arguments = invocation.getArguments();
		Ops operation = Ops.valueOf(method.getName());
		
		SerFunction<?,?> keyMapper = (SerFunction<?,?>) arguments[0];
		SerFunction<?,?> valueMapper = (SerFunction<?,?>) arguments[1];
		SerBinaryOperator<?> valueAggregator = operation.equals(Ops.reduceValues) 
				? (SerBinaryOperator<?>)arguments[2]
						: new BiFunctionToBinaryOperatorAdapter(Aggregators::aggregateToList);
				
		int operationId = this.currentStreamOperation == null ? 1 : this.currentStreamOperation.getId();
		String propertyName = DStreamConstants.MAP_SIDE_COMBINE + operationId + "_" + this.invocationPipeline.getSourceIdentifier();
		boolean mapSideCombine = Boolean.parseBoolean((String)this.executionConfig.getOrDefault(propertyName, "false"));

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SerFunction<?,?> kvMapper = new KeyValueMappingFunction(keyMapper, valueMapper, mapSideCombine ? valueAggregator : null);
		
		this.adjustCurrentStreamState();
		
		this.currentStreamOperation.addStreamOperationFunction(Ops.mapKeyValues.name(), kvMapper);

		DStreamOperation newStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		SerFunction<?,?> newStreamFunction = operation.equals(Ops.reduceValues) 
				? new ValuesReducingFunction<>(valueAggregator)
						: new ValuesAggregatingFunction<>(valueAggregator);
		newStreamOperation.addStreamOperationFunction(operation.name(), newStreamFunction);
		this.currentStreamOperation = newStreamOperation;
	}
	
	/**
	 * 
	 * @param invocation
	 */
	private void addStreamComparatorOperation(DStreamInvocation invocation){
		SerComparator<?> comparator = invocation.getArguments().length == 1 ? (SerComparator<?>)invocation.getArguments()[0] : null ;
		this.adjustCurrentStreamState();
		DStreamToStreamAdapterFunction streamAdaperFunc = new DStreamToStreamAdapterFunction(invocation.getMethod().getName(), comparator);
		this.currentStreamOperation.addStreamOperationFunction(invocation.getMethod().getName(), streamAdaperFunc);
	}

	/**
	 *
	 * @param invocation
	 */
	private void addStreamConsumerOperation(DStreamInvocation invocation){
		SerConsumer<?> consumer = invocation.getArguments().length == 1 ? (SerConsumer<?>)invocation
				.getArguments()[0] :
				null ;
		this.adjustCurrentStreamState();
		DStreamToStreamAdapterFunction streamAdaperFunc = new DStreamToStreamAdapterFunction(invocation.getMethod().getName(), consumer);
		this.currentStreamOperation.addStreamOperationFunction(invocation.getMethod().getName(), streamAdaperFunc);
	}
	
	/**
	 * 
	 */
	private void addStreamReduceOperation(DStreamInvocation invocation){	
		Ops operation = Ops.valueOf(invocation.getMethod().getName());
		SerFunction<?, Integer> keyMapper = s -> 0;
		SerFunction<?,?> valueMapper;
		SerBinaryOperator<?> valueAggregator;
		
		if (operation.equals(Ops.count)){
			valueMapper = s -> 1L;
			valueAggregator = (a, b) -> ((long)a) + ((long)b);
		} 
		else if (operation.equals(Ops.reduce)) {
			valueMapper = s -> s;
			valueAggregator = (SerBinaryOperator<?>) invocation.getArguments()[0];
		}
		else {
			throw new IllegalStateException("Unrecognized or unsupported operation: " + invocation.getMethod().getName());
		}
		@SuppressWarnings({ "unchecked", "rawtypes" })
		SerFunction<?,?> kvMapper = new KeyValueMappingFunction(keyMapper, valueMapper, valueAggregator);
		
		this.adjustCurrentStreamState();
		
		this.currentStreamOperation.addStreamOperationFunction(Ops.mapKeyValues.name(), kvMapper);

		SerFunction<Stream<Entry<Integer,Iterator<Long>>>,Stream<Long>> valueReducingFunction = 
				new ValuesReducingFunction<Integer, Long, Long>(valueAggregator);
		DStreamOperation valueReducingOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
			
		valueReducingOperation.addStreamOperationFunction(Ops.reduceValues.name(), valueReducingFunction);
		this.currentStreamOperation = valueReducingOperation;
		
		DStreamOperation mappingOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		mappingOperation.addStreamOperationFunction(Ops.map.name(), this.shuffleResultNormalizer);
		this.currentStreamOperation = mappingOperation;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({"unchecked", "rawtypes" })
	private AbstractStreamMergingFunction createStreamCombiner(String operationName, SerFunction firstStreamPreProcessingFunction) {
		return operationName.equals(Ops.join.name())
				? new StreamJoinerFunction(firstStreamPreProcessingFunction)
					: new StreamUnionFunction(operationName.equals(Ops.union.name()), firstStreamPreProcessingFunction);
	}
	
	/**
	 * Will ensure that:<br>
	 * if no stream operations were created, 
	 * it will create an initial one, essentially creating an execution stage.<br>
	 * If current operation is 'classify', it would create a post shuffle stage
	 */
	private void adjustCurrentStreamState(){
		if (this.currentStreamOperation == null){
			this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++);
		}
		
		if (this.currentStreamOperation.isClassify()){
			this.addPostShuffleNormalizer();
		}
	}
	
	/**
	 * Determines which unmap function to use. For cases when previous operation was 'classify'
	 * the unmap function is {@link #unmapFunction} otherwise it will produce a pass thru function.
	 */
	private SerFunction<?,?> determineUnmapFunction(String lastOperationName){
		return Ops.classify.name().equals(lastOperationName) ? this.shuffleResultNormalizer : s -> s; 
	}
	
	/**
	 * 
	 */
	private void addPostShuffleNormalizer(){
		this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++, this.currentStreamOperation);
		this.currentStreamOperation.addStreamOperationFunction(Ops.load.name(), this.shuffleResultNormalizer);
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
	 * Will create an operation named 'extract' with a pass-thru function
	 */
	private void createDefaultExtractOperation(){
		this.currentStreamOperation = new DStreamOperation(this.operationIdCounter++);
		this.currentStreamOperation.addStreamOperationFunction(Ops.extract.name(), s -> s);
	}
}
