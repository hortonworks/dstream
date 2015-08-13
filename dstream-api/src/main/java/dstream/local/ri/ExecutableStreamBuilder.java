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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import dstream.DStreamConstants;
import dstream.StreamOperation;
import dstream.StreamOperations;
import dstream.function.GroupingFunction;
import dstream.function.HashGroupingFunction;
import dstream.function.SerializableFunctionConverters.SerFunction;
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
final class ExecutableStreamBuilder {
	
	private final StreamOperations streamOperations;
	
	private final Properties executionConfig;
	
	private final String executionName;
	
	private Stream<?> executionStream;
	
	private final GroupingFunction grouper;
	
	public ExecutableStreamBuilder(String executionName, StreamOperations streamOperations, Properties executionConfig){
		this.streamOperations = streamOperations;
		this.executionConfig = executionConfig;
		this.executionName = executionName;
		this.grouper = this.determineGrouper();
	}

	@SuppressWarnings("rawtypes")
	public Stream<?> build(){
		

		this.executionStream = this.createInitialStream(streamOperations.getPipelineName());
		
		if (streamOperations.getOperations().isEmpty()){
			Stream<Entry<Integer, Stream>> shuffledPartitionStream = this.partitionStream(this.executionStream);
			this.executionStream = shuffledPartitionStream;
			boolean mapPartitions = false; //TODO fix when mapPartition is supported
			this.applyFuncOnEachPartition(shuffledPartitionStream, mapPartitions, s -> s);
			return this.executionStream;
		}
		else {
			this.doBuild();
		}
		return this.executionStream;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void doBuild() {
		Iterator<StreamOperation> streamOperationsIter = streamOperations.getOperations().iterator();
		
		boolean needsPartitioning = false;
		boolean needsLastPartition = true;
		while (streamOperationsIter.hasNext()) {
			StreamOperation streamOperation = (StreamOperation) streamOperationsIter.next();
			SerFunction streamFunction = streamOperation.getStreamOperationFunction();
			if (streamOperation.getGroupClassifier() != null){
				this.grouper.setClassifier((SerFunction<Object, ?>) streamOperation.getGroupClassifier());
			}
			if (needsPartitioning){
				boolean mapPartitions = false;//TODO fix when mapPartition is supported
				
				Stream<Entry<Integer, Stream>> shuffledPartitionStream = this.partitionStream(this.executionStream);
				
				this.applyFuncOnEachPartition(shuffledPartitionStream, mapPartitions, streamFunction);
				
				if (streamOperationsIter.hasNext()) {
					this.executionStream = ((Stream<Stream<?>>) this.executionStream).reduce(Stream::concat).get();
				}
				needsLastPartition = false;
			}
			else {
				if (streamOperation.getLastOperationName().equals("join") || streamOperation.getLastOperationName().equals("union") ){
					List<StreamOperations> dependentOperations = streamOperation.getDependentStreamOperations();

					List<Stream<?>> dependentStreamsList = new ArrayList<>();
					dependentStreamsList.add(this.executionStream);
					for (StreamOperations streamOperations : dependentOperations) {
						ExecutableStreamBuilder executionBuilder = new ExecutableStreamBuilder(executionName, streamOperations, executionConfig);
						Stream<?> dependentStream = ((Stream<Stream<?>>)executionBuilder.build()).reduce(Stream::concat).get();	
						dependentStreamsList.add(dependentStream);
					}
					this.executionStream = dependentStreamsList.stream();
					needsPartitioning = false;
				}
				this.executionStream = (Stream<?>) streamFunction.apply(this.executionStream);
				needsPartitioning = true;		
			}
		}
		
		if (needsLastPartition){ // for cases where there is no explicit shuffle operations
			boolean mapPartitions = false;//TODO fix when mapPartition is supported
			Stream<Entry<Integer, Stream>> shuffledPartitionStream = this.partitionStream(this.executionStream);
			this.applyFuncOnEachPartition(shuffledPartitionStream, mapPartitions, s -> s);
		}
	}
	
	/**
	 * 
	 * @param shuffledPartitionStream
	 * @param mapPartitions
	 * @param partitionFunction
	 */
	@SuppressWarnings("rawtypes")
	private void applyFuncOnEachPartition(Stream<Entry<Integer, Stream>> shuffledPartitionStream, boolean mapPartitions, SerFunction<Stream<?>, Stream<?>> partitionFunction) {
		if (mapPartitions){
			throw new IllegalStateException("Currently not supported");
		}
		else {
			// essentially creating a lazy shuffle
			this.executionStream = shuffledPartitionStream.map(partition -> 
					partitionFunction.apply(partition.getValue()).sorted()).parallel();
		}
	}
	/**
	 * 
	 */
	private Stream<?> createInitialStream(String pipelineName){
		String sourceProperty = executionConfig.getProperty(DStreamConstants.SOURCE + pipelineName);
		Assert.notEmpty(sourceProperty, DStreamConstants.SOURCE + pipelineName +  "' property can not be found in " + 
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
	 * @param streamToShuffle
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Stream<Entry<Integer, Stream>> partitionStream(Stream<?> streamToShuffle){
		
		// Partitions elements
		Stream<Tuple2<Integer, Object>> partitionedStream = streamToShuffle
			.map(element -> tuple2(this.grouper.apply(element), element)); 

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
//		this.shuffled = true;
		return normalizedPartitionStream;
	}
	
	/**
	 * 
	 */
	private GroupingFunction determineGrouper(){
		String parallelizmProp = this.executionConfig.getProperty(DStreamConstants.PARALLELISM);
		String partitionerProp = this.executionConfig.getProperty(DStreamConstants.GROUPER);
		
		int parallelism = parallelizmProp == null ? 1 : Integer.parseInt(parallelizmProp);
	
		return partitionerProp != null 
				? ReflectionUtils.newInstance(partitionerProp, new Class[]{int.class}, new Object[]{parallelism}) 
						: new HashGroupingFunction(parallelism);
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
