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
package io.dstream.local.ri;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.dstream.DStreamConstants;
import io.dstream.DStreamExecutionGraph;
import io.dstream.DStreamOperation;
import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.local.ri.ShuffleHelper.RefHolder;
import io.dstream.support.AbstractPartitionedStreamProducingSourceSupplier;
import io.dstream.support.Aggregators;
import io.dstream.support.Classifier;
import io.dstream.support.HashClassifier;
import io.dstream.support.PartitionIdHelper;
import io.dstream.support.SourceSupplier;
import io.dstream.support.UriSourceSupplier;
import io.dstream.utils.KVUtils;
import io.dstream.utils.ReflectionUtils;
import io.dstream.utils.SingleValueIterator;

/**
 *
 *
 */
final class LocalDStreamExecutionEngine {

	private final Properties executionConfig;

	private final String executionName;

	private final Classifier classifier;

	private List<List<?>> realizedStageResults;

	private final ThreadLocal<Integer> partitionIdHolder;

	@SuppressWarnings("unchecked")
	public LocalDStreamExecutionEngine(String executionName, Properties executionConfig){
		this.executionName = executionName;
		this.executionConfig = executionConfig;
		this.classifier = this.determineClassifier();
		try {
			Field tl = ReflectionUtils.findField(PartitionIdHelper.class, "partitionIdHolder", ThreadLocal.class);
			tl.setAccessible(true);
			this.partitionIdHolder = (ThreadLocal<Integer>) tl.get(null);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 *
	 */
	public Stream<Stream<?>> execute(DStreamExecutionGraph pipeline) {
		return this.execute(pipeline, false);
	}

	/**
	 *
	 */
	private Stream<Stream<?>> execute(DStreamExecutionGraph pipeline, boolean partition) {
		List<DStreamOperation> streamOperations = pipeline.getOperations();

		for (int i = 0; i < streamOperations.size(); i++) {
			this.doExecuteStage(streamOperations.get(i), partition, pipeline.getName());
		}

		return this.realizedStageResults.stream().map(list -> list.stream());
	}

	/**
	 *
	 * @param streamOperation
	 * @param partition
	 */
	@SuppressWarnings("unchecked")
	private void doExecuteStage(DStreamOperation streamOperation, boolean partition, String pipelineName){
		SerFunction<Stream<?>, Stream<?>> streamFunction = streamOperation.getStreamOperationFunction();

		if (this.realizedStageResults == null){
			List<List<?>> realizedIntermediateResult = Stream.of( streamFunction.apply(this.createInitialStream(pipelineName)) )
					.map(stream -> stream.collect(Collectors.toList()))
					.collect(Collectors.toList());

			if (partition){
				Stream<?> mergedStream = realizedIntermediateResult.stream().map(list -> ((Stream<Object>)list.stream())).reduce((a,b) -> Stream.concat(a, b)).get();
				Stream<Entry<Integer, List<Object>>> partitionedStreamResult = this.partitionStream(mergedStream);
				Stream<Stream<?>> partitionedStreamResultNoId = this.unmapPartitions(partitionedStreamResult);
				realizedIntermediateResult = partitionedStreamResultNoId.map(stream -> stream.collect(Collectors.toList())).collect(Collectors.toList());
			}

			this.realizedStageResults = realizedIntermediateResult;
		}
		else {
			Stream<?> mergedStream = this.realizedStageResults.stream().map(list -> ((Stream<Object>)list.stream())).reduce((a,b) -> Stream.concat(a, b)).get();

			Stream<Entry<Integer, List<Object>>> partitionedStreamResult = this.partitionStream(mergedStream);
			Stream<Stream<?>> partitionedStreamResultNoId = this.unmapPartitions(partitionedStreamResult);

			if (streamOperation.getCombinableExecutionGraphs().size() > 0){
				List<Stream<?>> currentPartitions = partitionedStreamResultNoId.collect(Collectors.toList());
				Map<Integer, Object> matchedPartitions = new LinkedHashMap<>();
				for (int i = 0; i < currentPartitions.size(); i++) {
					matchedPartitions.merge(i, currentPartitions.get(i), Aggregators::aggregateToList);
				}

				List<DStreamExecutionGraph> dependentPipelines = streamOperation.getCombinableExecutionGraphs();
				for (DStreamExecutionGraph dependentPipeline : dependentPipelines) {
					LocalDStreamExecutionEngine e = new LocalDStreamExecutionEngine(this.executionName, this.executionConfig);
					Stream<Stream<?>> dependentStream = e.execute(dependentPipeline, true);
					List<Stream<?>> dependentPartitions = dependentStream.collect(Collectors.toList());
					for (int i = 0; i < dependentPartitions.size(); i++) {
						matchedPartitions.merge(i, dependentPartitions.get(i), Aggregators::aggregateToList);
					}
				}
				partitionedStreamResultNoId = matchedPartitions.values().stream().map(list -> ((List<?>)list).stream());
			}

			Stream<Stream<?>> transformedStreams = partitionedStreamResultNoId.map(stream -> streamFunction.apply(stream));
			List<List<?>> realizedIntermediateResult = transformedStreams.map(stream -> stream.collect(Collectors.toList())).collect(Collectors.toList());
			this.realizedStageResults = realizedIntermediateResult;
		}
	}

	/**
	 *
	 * @param shuffledPartitionStream
	 * @return
	 */
	private Stream<Stream<?>> unmapPartitions(Stream<Entry<Integer, List<Object>>> shuffledPartitionStream) {
		return shuffledPartitionStream.map(entry -> entry.getValue().stream().map(val -> { this.partitionIdHolder.set(entry.getKey());  return val;}));
	}

	/**
	 *
	 * @param pipelineName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <R> Stream<R> createInitialStream(String pipelineName){
		SourceSupplier<R> sourceSupplier = SourceSupplier.<R> create(this.executionConfig, pipelineName, null);
		if (sourceSupplier instanceof UriSourceSupplier) {
			UriSourceSupplier uriSupplier = (UriSourceSupplier) sourceSupplier;
			Stream<URI> uriSources = uriSupplier.get();
			return (Stream<R>) uriSources.map(this::buildStreamFromURI).reduce(Stream::concat).get();
		}
		else if (sourceSupplier instanceof AbstractPartitionedStreamProducingSourceSupplier) {
			AbstractPartitionedStreamProducingSourceSupplier<R> spSourceSupplier = (AbstractPartitionedStreamProducingSourceSupplier<R>) sourceSupplier;
			return spSourceSupplier.get();
		}
		else {
			throw new IllegalStateException("Unsupported SourceSupplier " + sourceSupplier.getClass().getName());
		}
	}

	/**
	 *
	 */
	private Stream<String> buildStreamFromURI(URI uri) {
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Stream<Entry<Integer, List<Object>>> partitionStream(Stream<?> streamToShuffle){
		//		Map collectedPartitions = streamToShuffle.collect(Collectors.groupingBy(element -> this.classifier.getClassificationId(element), Collectors.toList()));
		//		Stream<Entry<Integer, List<Object>>> groupedPartitionsStream = collectedPartitions.entrySet().stream();
		//		return groupedPartitionsStream;

		Stream<Entry<Integer, Object>> partitionedStream = streamToShuffle
				.map(element -> KVUtils.kv(this.classifier.getClassificationId(element), element));


		/*
		 * Groups elements for each partition using ShuffleHelper
		 * If an element is a Key/Value Entry, then ShuffleHelper will group it as Key/List[Values]
		 * 		The resulting partition entry will look like this: {0={key1=[v,v,v,v],key2=v}}
		 * If an element is not a Key/Value Entry,then values will be grouped into a List - List[Values]
		 * 		The resulting partition entry will look like this: {0=[v1,v2,v1,v3],v4}
		 */
		Stream<Map<Integer, ?>> groupedPartitionsStream = Stream.of(partitionedStream)
				.map(stream -> stream.collect(Collectors.toMap((Entry<Integer, Object> s) -> s.getKey(), s -> (Object)new RefHolder(s.getValue()), ShuffleHelper::group)));

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
}
