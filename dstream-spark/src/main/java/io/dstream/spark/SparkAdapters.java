/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dstream.spark;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.dstream.SerializableStreamAssets.SerFunction;
import io.dstream.support.Aggregators;
import io.dstream.utils.KVUtils;
import io.dstream.utils.SingleValueIterator;

/**
 * 
 */
public abstract class SparkAdapters {

	/**
	 * 
	 * @param inStream
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Stream<scala.Tuple2<Object,Object>> tuple2ToScalaTuple2(Stream<Entry<Object, Object>> inStream) {
		return inStream.map(entry -> new scala.Tuple2(entry.getKey(), entry.getValue()));
	}
	
	/**
	 * 
	 * @param inStream
	 */
	public static  Stream<Entry<Object, ? extends Iterator<? extends Object>>> groupTuple2Values(Stream<scala.Product2<Object,Object>> inStream){
		Map<Object, Object> groupedMap = inStream.collect(Collectors.toMap(t2 -> t2._1(), t2 -> t2._2(), Aggregators::aggregateToList));
		
		Stream<Entry<Object, ? extends Iterator<? extends Object>>> groupedStream = groupedMap.entrySet().stream().map(entry -> {
			if (entry.getValue() instanceof List){
				return KVUtils.kv(entry.getKey(), ((List<?>)entry.getValue()).iterator());
			}
			else {
				return KVUtils.kv(entry.getKey(), new SingleValueIterator<>(entry.getValue()));
			}
		});
		
		return groupedStream;
	}
	
	/**
	 * 
	 * @param partitionedResults
	 */
	public static <T> Stream<Stream<T>> toResultStream(Stream<T>[] partitionedResults) {
		return Stream.of(partitionedResults);
	}
	
	/**
	 * 
	 * @param inputIterator
	 * @param streamFunction
	 */
	public static Stream<?> processSourceStreamRDD(Iterator<?> inputIterator, SerFunction<Stream<?>, Stream<Entry<Object, Object>>> streamFunction){
		Stream<?> inputStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(inputIterator, Spliterator.ORDERED), false);
		Stream<?> resultStream = streamFunction.apply(inputStream).map(entry -> new scala.Tuple2(entry.getKey(), entry.getValue()));
		return resultStream;
	}
	
	/**
	 * 
	 * @param inputIterators
	 * @param streamCombineFunc
	 */
	public static Stream<?> combineStreams(Iterator<scala.Product2<Object,Object>>[] inputIterators, SerFunction<Stream<Stream<?>>, Stream<?>> streamCombineFunc) {
		Stream<Stream<?>> groupedStreams = Stream.of(inputIterators)
			.map(iter -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false))
			.map(stream -> groupTuple2Values(stream));
		return streamCombineFunc.apply(groupedStreams);
	}
}
