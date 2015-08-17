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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.DStreamConstants;
import dstream.StreamOperation;
import dstream.StreamOperations;
import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.local.ri.ShuffleHelper.RefHolder;
import dstream.support.Classifier;
import dstream.support.HashClassifier;
import dstream.support.SourceSupplier;
import dstream.utils.Assert;
import dstream.utils.KVUtils;
import dstream.utils.ReflectionUtils;

/**
 * 
 *
 */
final class ExecutableStreamBuilder {
	
//	private final StreamOperations streamOperations;
	
	private final Properties executionConfig;
	
	private final String executionName;
	
	private Stream<?> executionStream;
	
	private final Classifier classifier;
	
	private List<Stream<Entry<Integer, List<Object>>>> partitionedStreams = new ArrayList<>();
	
	public ExecutableStreamBuilder(String executionName, String pipelineName, Properties executionConfig){
//		this.streamOperations = streamOperations;
		this.executionStream = this.createInitialStream(pipelineName);
		this.executionConfig = executionConfig;
		this.executionName = executionName;
		this.classifier = this.determineClassifier();
	}

	/**
	 * 
	 * @return
	 */
	public Stream<?> build(StreamOperation operation){
//		this.executionStream = this.createInitialStream(this.streamOperations.getPipelineName());
//		this.doBuild(this.streamOperations.getOperations().iterator());
		return this.executionStream;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void doBuild(Iterator<StreamOperation> streamOperationsIter) {
		boolean needsPartitioning = false;
//		while (streamOperationsIter.hasNext()) {
//			StreamOperation streamOperation = (StreamOperation) streamOperationsIter.next();
//			SerFunction streamFunction = streamOperation.getStreamOperationFunction();
//			
//			if (needsPartitioning){
//				boolean mapPartitions = false;//TODO fix when mapPartition is supported			
//				Stream<Entry<Integer, List<Object>>> shuffledPartitionStream = this.partitionStream(this.executionStream.sequential());
//				partitionedStreams.add(shuffledPartitionStream);
//				
//				if (streamOperation.getDependentStreamOperations().size() > 0){
//					List<Stream<?>> dependentStreamsList = new ArrayList<>();
//					List<StreamOperations> dependentOperations = streamOperation.getDependentStreamOperations();
//					for (StreamOperations streamOperations : dependentOperations) {
//						ExecutableStreamBuilder executionBuilder = new ExecutableStreamBuilder(this.executionName, streamOperations, this.executionConfig);
//						
//						Stream<?> dependentStream = ((Stream<Stream<?>>)executionBuilder.build()).reduce(Stream::concat).get();	
//						dependentStreamsList.add(dependentStream);
//					}
////					
////					
////					
////					
////					
////					Stream<?> postProcessedShuffledStream = this.postProcessPartitions(shuffledPartitionStream, mapPartitions);				
//////	
////					Stream<?> streamsToCombine = this.gatherStreamsToCombine(streamOperation, postProcessedShuffledStream);
//////					
//////					Stream<?> combinedStreams = (Stream<?>) streamFunction.apply(streamsToCombine); // join/union
//////
//////					shuffledPartitionStream = this.partitionStream(combinedStreams);
//////					this.executionStream = this.postProcessPartitions(shuffledPartitionStream, mapPartitions);
//				}
//				else {
////					if (!mapPartitions){
////						Stream<Stream<?>> partitionsNoId = this.unmapPartitions(shuffledPartitionStream, mapPartitions);
////						this.executionStream = partitionsNoId.map(stream -> streamFunction.apply(stream));
////					}
////					else {
////						throw new IllegalStateException("Not supported at the moment");
////					}
//				}
//				
////				if (streamOperationsIter.hasNext()) {
////					this.executionStream = ((Stream<Stream<?>>) this.executionStream).reduce(Stream::concat).get();
////				}			
//			}
//			else {
//				this.executionStream = (Stream<?>) streamFunction.apply(this.executionStream.parallel());
//				needsPartitioning = true;		
//			}
//		}
//		if (this.partitionedStreams.size() > 0){
//			
//		}
	}
	
	/**
	 * 
	 * @param streamOperation
	 * @return
	 */
//	@SuppressWarnings("unchecked")
//	private Stream<?> gatherStreamsToCombine(StreamOperation streamOperation, Stream<?> thisStream){	
//		// merge partitions
//		thisStream = ((Stream<Stream<?>>) thisStream).reduce(Stream::concat).get();
//		List<Stream<?>> dependentStreamsList = new ArrayList<>();
//		dependentStreamsList.add(thisStream);
//		
//		List<StreamOperations> dependentOperations = streamOperation.getDependentStreamOperations();
//		for (StreamOperations streamOperations : dependentOperations) {
//			ExecutableStreamBuilder executionBuilder = new ExecutableStreamBuilder(this.executionName, streamOperations, this.executionConfig);
//			
//			Stream<?> dependentStream = ((Stream<Stream<?>>)executionBuilder.build()).reduce(Stream::concat).get();	
//			dependentStreamsList.add(dependentStream);
//		}
//		return dependentStreamsList.stream();
//	}
	
//	/**
//	 * 
//	 */
//	private Stream<?> postProcessPartitions(Stream<Entry<Integer, List<Object>>> shuffledPartitionStream, boolean mapPartitions) {
//		return this.postProcessPartitions(shuffledPartitionStream, mapPartitions, null);
//	}
	
	/**
	 * 
	 * @param shuffledPartitionStream
	 * @param mapPartitions
	 * @param partitionFunction
	 */
	@SuppressWarnings("unchecked")
	private Stream<Stream<?>> unmapPartitions(Stream<Entry<Integer, List<Object>>> shuffledPartitionStream, boolean mapPartitions) {
//		if (mapPartitions){
//			throw new IllegalStateException("Currently not supported");
//		}
//		else {
//			// essentially creating a lazy shuffle
			return shuffledPartitionStream
					.map(entry -> entry.getValue().stream());
//					.map(stream -> {
//						if (postPartitionFunction != null){
//							stream = (Stream<Object>) postPartitionFunction.apply(stream);
//						}
//						return stream.sorted();
//					});
//		}
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
	private Stream<Entry<Integer, List<Object>>> partitionStream(Stream<?> streamToShuffle){

		// Partitions elements
		Stream<Entry<Integer, Object>> partitionedStream = streamToShuffle
			.map(element -> KVUtils.kv(this.classifier.getClassificationId(element), element)); 
		
		/*
		 * Groups elements for each partition using ShuffleHelper
		 * If an element is a Key/Value Entry, then ShuffleHelper will group it as Key/List[Values]
		 * 		The resulting partition entry will look like this: {0={key1=[v,v,v,v],key2=v}}
		 * If an element is not a Key/Value Entry,then values will be grouped into a List - List[Values]
		 * 		The resulting partition entry will look like this: {0=[v1,v2,v1,v3],v4}
		 */
		Stream<Map<Integer, Object>> groupedPartitionsStream = Stream.of(partitionedStream)
				.map(stream -> stream.collect(Collectors.toMap((Entry<Integer, Object> s) -> s.getKey(), s -> new RefHolder(s.getValue()), ShuffleHelper::group)));
		
//		Map<Integer, Object> groupedPartitions = partitionedStream
//				.collect(Collectors.toMap((Entry<Integer, Object> s) -> s.getKey(), s -> new RefHolder(s.getValue()), ShuffleHelper::group));
//		
//		Stream<Map<Integer, Object>> groupedPartitionsStream = Stream.of(groupedPartitions);
	
		Stream<Entry<Integer, List<Object>>> normalizedPartitionStream = groupedPartitionsStream.flatMap(map -> map.entrySet().stream()).map(entry -> {
			Object value = entry.getValue();		
			Entry<Integer, List<Object>> normalizedEntry = null;
			
			if (value instanceof RefHolder){
				Object realValue = ((RefHolder) value).ref;
				if (realValue instanceof Entry){
					value = Stream.of((Entry) realValue).collect(Collectors.toMap(e -> e.getKey(), e -> Collections.singletonList(e.getValue())));
				}
				else {
					value = Stream.of(realValue).collect(Collectors.toList());
				}
			}
		  
			if (value instanceof Map){
				Map vMap = (Map) value;
				vMap.forEach((k,v) -> vMap.replace(k, v instanceof List ? ((List)v).iterator() : new SingleValueIterator(v) ));
				TreeMap<Object, Object> sortedMap = new TreeMap<>(vMap);
				normalizedEntry = KVUtils.kv(entry.getKey(), new ArrayList<>(sortedMap.entrySet()));
			}
			else {
				normalizedEntry = KVUtils.kv(entry.getKey(), (List)value);
			}
			
			return normalizedEntry;
		});
		return normalizedPartitionStream;
	}
	
	/**
	 * 
	 */
	private Classifier determineClassifier(){
		String parallelizmProp = this.executionConfig.getProperty(DStreamConstants.PARALLELISM);
		String partitionerProp = this.executionConfig.getProperty(DStreamConstants.CLASSIFIER);
		
		int parallelism = parallelizmProp == null ? 1 : Integer.parseInt(parallelizmProp);
	
		return partitionerProp != null 
				? ReflectionUtils.newInstance(partitionerProp, new Class[]{int.class}, new Object[]{parallelism}) 
						: new HashClassifier(parallelism);
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
