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
package dstream.local.ri;

import static dstream.utils.Tuples.Tuple2.tuple2;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import dstream.DStreamConstants;
import dstream.DStreamInvocation;
import dstream.DStreamInvocationPipeline;
import dstream.function.BiFunctionToBinaryOperatorAdapter;
import dstream.function.DStreamToStreamAdapterFunction;
import dstream.function.HashPartitionerFunction;
import dstream.function.KeyValueMappingFunction;
import dstream.function.PartitionerFunction;
import dstream.function.SerializableFunctionConverters.SerBiFunction;
import dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.function.StreamJoinerFunction;
import dstream.function.ValuesAggregatingFunction;
import dstream.function.ValuesReducingFunction;
import dstream.local.ri.ShuffleHelper.RefHolder;
import dstream.support.SourceSupplier;
import dstream.utils.Assert;
import dstream.utils.KVUtils;
import dstream.utils.ReflectionUtils;
import dstream.utils.Tuples.Tuple2;

/**
 * 
 *
 */
public class ExecutableStreamBuilder {
	private final String executionName;
	
	private final DStreamInvocationPipeline invocationPipeline;
	
	private final Properties executionConfig;
	
	private final PartitionerFunction<Object> partitioner;
	
	private Stream<?> sourceStream;
	
	private boolean shuffled;
	
	public ExecutableStreamBuilder(String executionName, DStreamInvocationPipeline invocationPipeline, Properties executionConfig){
		this.executionName = executionName;
		this.invocationPipeline = invocationPipeline;
		this.executionConfig = executionConfig;
		this.partitioner = this.determinePartitioner();
		
		this.sourceStream = this.createInitialStream();
	}
	
	/**
	 * 
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Stream<?> build(){
//		boolean mapPartitions = invocation.isMapPartition();
		boolean mapPartitions = false;
		
		for (DStreamInvocation invocation : invocationPipeline.getInvocations()) {
			String operationName = invocation.getMethod().getName();
			if (this.shuffled){
				this.sourceStream = ((Stream<Stream<?>>)this.sourceStream).reduce(Stream::concat).get();
				this.shuffled = false;
			}
			if (this.isTransformation(operationName)){
				this.sourceStream = new DStreamToStreamAdapterFunction(operationName, invocation.getArguments()[0]).apply(this.sourceStream);
			}
			else if (this.isShuffle(operationName)){
				if (operationName.equals("reduceGroups") || operationName.equals("aggregateGroups")){	
					SerFunction keyMapper = (SerFunction) invocation.getArguments()[0];
					SerFunction valueMapper = (SerFunction) invocation.getArguments()[1];
					SerBinaryOperator reducer = operationName.equals("reduceGroups") 
							? (SerBinaryOperator) invocation.getArguments()[2]
									: new BiFunctionToBinaryOperatorAdapter((SerBiFunction)invocation.getArguments()[2]);
											
					ValuesReducingFunction<Object, Object, Object> aggregatingFunction = operationName.equals("reduceGroups") 
							? new ValuesReducingFunction<>(reducer)
									: new ValuesAggregatingFunction<>(reducer);							
					KeyValueMappingFunction keyValueMappingFunction = new KeyValueMappingFunction<>(keyMapper, valueMapper);
					
					Stream<Entry<Object, Object>> keyValuesStream = keyValueMappingFunction.apply(this.sourceStream);

					Stream<Entry<Integer, Stream>> shuffledPartitionStream = this.shuffleStream(keyValuesStream);
					if (mapPartitions){
						throw new IllegalStateException("Currently not supported");
					}
					else {
						this.sourceStream = shuffledPartitionStream.map(entry -> 
							aggregatingFunction.apply(	(Stream<Entry<Object, Iterator<Object>>>) entry.getValue()	).sorted());
					}
				}
				else if (operationName.equals("join") || operationName.equals("union")) {
					DStreamInvocationPipeline dependentInvocationChain = (DStreamInvocationPipeline) invocation.getArguments()[0];
					
					int joiningStreamsSize = dependentInvocationChain.getStreamType().getTypeParameters().length;
					ExecutableStreamBuilder dependentBuilder = new ExecutableStreamBuilder(this.executionName, dependentInvocationChain, this.executionConfig);

					Optional<Stream<?>> oThat = ((Stream<Stream<?>>)dependentBuilder.build()).reduce(Stream::concat);
					Optional<Stream> oThis = (this.shuffleStream(this.sourceStream).map(entry -> entry.getValue())).reduce(Stream::concat);
					
					Stream<?> thisStream = oThis.get();
					Stream<?> thatStream = oThat.get();
							
					StreamJoinerFunction streamJoiner = new StreamJoinerFunction();
					streamJoiner.addCheckPoint(joiningStreamsSize);
					if (invocation.getSupplementaryOperation() != null){
						streamJoiner.addTransformationOrPredicate("filter", invocation.getSupplementaryOperation());
					}
					
					Stream<?> joinedStream = streamJoiner.apply(Stream.of(thisStream, thatStream));
					
					Stream<Entry<Integer, Stream>> shuffledPartitionStream = this.shuffleStream(joinedStream);
					
					if (mapPartitions){
						throw new IllegalStateException("Currently not supported");
					}
					else {
						this.sourceStream = shuffledPartitionStream.map(entry -> entry.getValue());
					}
				}
				else {
					throw new IllegalStateException("Operation is not supported at the moment: " + operationName);
				}
			}
		}
		
		if (!this.shuffled){
			Stream<Entry<Integer, Stream>> shuffledPartitionedStream = this.shuffleStream(this.sourceStream);
			if (mapPartitions){
				this.sourceStream = shuffledPartitionedStream;
				throw new IllegalStateException("Currently not supported");
			}
			else {
				this.sourceStream = shuffledPartitionedStream.map(entry -> entry.getValue());
			}
		}
		
		return this.sourceStream;
	}
	
	/**
	 * 
	 */
	private Stream<?> buildStreamFromURI(URI uri) {
		try {
			return Files.lines(Paths.get(uri));
		} 
		catch (Exception e) {
			throw new IllegalStateException("Failed to create Stream from URI: " + uri, e);
		}
	}
	
	/**
	 * 
	 */
	private Stream<?> createInitialStream(){
		String sourceProperty = executionConfig.getProperty(DStreamConstants.SOURCE + invocationPipeline.getSourceIdentifier());
		Assert.notEmpty(sourceProperty, DStreamConstants.SOURCE + invocationPipeline.getSourceIdentifier() +  "' property can not be found in " + 
				executionName + ".cfg configuration file.");
		
		SourceSupplier<?> sourceSupplier = SourceSupplier.create(sourceProperty, null);
		Object[] sources = sourceSupplier.get();
		Assert.notEmpty(sources, "sources must not be null or empty");
		
		if (sources[0] instanceof URI){
			URI[] uriSources = Arrays.copyOf(sources, sources.length, URI[].class);
			return Stream.of(uriSources).map(this::buildStreamFromURI).reduce(Stream::concat).get();
		}
		else {
			throw new IllegalStateException("SourceSuppliers other then URISourceSupplier are not supported at the moment");
		}
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
		return operationName.equals("reduceGroups") ||
			   operationName.equals("aggregateGroups") ||
			   operationName.equals("join") ||
			   operationName.equals("union") ||
			   operationName.equals("unionAll") ||
			   operationName.equals("partition");
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Stream<Entry<Integer, Stream>> shuffleStream(Stream<?> streamToShuffle){
		
		// Partitions elements
		Stream<Tuple2<Integer, Object>> partitionedStream = streamToShuffle
			.map(element -> tuple2(this.partitioner.apply(element), element)); 

		/*
		 * Groups elements for each partition using ShuffleHelper
		 * If an element is a Key/Value Entry, then ShuffleHelper will group it as Key/List[Values]
		 * 		The resulting partition entry will look like this: {0={key1=[v,v,v,v],key2=[v,v,v]}}
		 * If an element is not a Key/Value Entry,then values will be grouped into a List - List[Values]
		 * 		The resulting partition entry will look like this: {0=[v1,v2,v1,v3],[v4,v6,v0]}
		 */
		Stream<Map<Integer, Object>> groupedValuesStream = Stream.of(partitionedStream)
				.map(stream -> stream.collect(Collectors.toMap((Tuple2<Integer, Object> s) -> s._1(), s -> new RefHolder(s._2()), ShuffleHelper::group)));		
		
		Stream<Entry<Integer, Stream>> normalizedPartitionStream = groupedValuesStream.flatMap(map -> map.entrySet().stream()).map(entry -> {
			Object value = entry.getValue();
			
			if (value instanceof RefHolder){
				Object realValue = ((RefHolder) value).ref;
				if (realValue instanceof Entry){
					Entry e = (Entry) realValue;
					Map m = new HashMap<>();
					m.put(e.getKey(), Collections.singletonList(e.getValue()));
					value = m;
				}
				else {
					value = Arrays.asList(new Object[]{realValue});
				}
			}
			
			Entry<Integer, Stream> normalizedEntry;
			if (value instanceof Map){
				Map<Object, Object> vMap = (Map<Object, Object>) value;
				vMap.forEach((k,v) -> vMap.replace(k, v instanceof List ? ((List)v).iterator() : new SingleValueIterator(v) ));
				normalizedEntry = KVUtils.kv(entry.getKey(), vMap.entrySet().stream());
			}
			else {
				normalizedEntry = KVUtils.kv(entry.getKey(), StreamSupport.stream( ((Iterable<?>)value).spliterator(), false));
			}
			
			return normalizedEntry;
		});
		this.shuffled = true;
		return normalizedPartitionStream;
	}
	
	/**
	 * 
	 */
	private PartitionerFunction<Object> determinePartitioner(){
		String parallelizmProp = this.executionConfig.getProperty(DStreamConstants.PARALLELISM);
		String partitionerProp = this.executionConfig.getProperty(DStreamConstants.PARTITIONER);
		
		int parallelism = parallelizmProp == null ? 1 : Integer.parseInt(parallelizmProp);
	
		return partitionerProp != null 
				? ReflectionUtils.newInstance(partitionerProp, new Class[]{int.class}, new Object[]{parallelism}) 
						: new HashPartitionerFunction<>(parallelism);
	}
	
	/**
	 */
	private static class SingleValueIterator<T> implements Iterator<T>{
		T v;
		boolean hasNext = true;
		SingleValueIterator(T v) {
			this.v = v;
		}
		@Override
		public boolean hasNext() {
			return hasNext;
		}
		@Override
		public T next() {
			hasNext = false;
			return v;
		}
	}
}
