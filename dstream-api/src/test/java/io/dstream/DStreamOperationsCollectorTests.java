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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import io.dstream.DStream;
import io.dstream.DStreamExecutionGraph;
import io.dstream.DStream.DStream2;
import io.dstream.utils.Tuples.Tuple2;

public class DStreamOperationsCollectorTests {

	private String streamName = DStreamOperationsCollectorTests.class.getSimpleName();
	
	@Test(expected=IllegalArgumentException.class)
	public void failNullElementType() throws Exception {
		DStream.ofType(null, "failNullElementType");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void failNoName() throws Exception {
		DStream.ofType(Object.class, "");
	}
	
	@Test
	public void validateConstruction() throws Exception {
		DStream<Object> pipeline = DStream.ofType(Object.class, "validateConstruction");
		assertEquals("validateConstruction", pipeline.getName());
	}
	
	@Test
	public void validateToString() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "validateToString").filter(s -> true).map(s -> s);
		assertEquals("validateToString:[filter, map]", stream.toString());
	}

	@Test
	public void validateRawStreamExecution() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "validateRawStreamExecution");
		Future<Stream<Stream<Object>>> resultFuture = stream.executeAs(this.streamName);
		
		Stream<Stream<Object>> result = resultFuture.get();	
		List<Stream<Object>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		DStreamExecutionGraph chainAccessor = (DStreamExecutionGraph) partitionStreams.get(0);
		assertEquals(2, chainAccessor.getOperations().size());
		assertEquals("extract", chainAccessor.getOperations().get(0).getLastOperationName());
		assertEquals("load", chainAccessor.getOperations().get(1).getLastOperationName());
		result.close();
	}
	
	@Test
	public void validateStreamInstanceIndependence() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "validateStreamInstanceIndependence");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		Future<Stream<Stream<Object>>> resultFutureA = streamA.executeAs(this.streamName);
		Future<Stream<Stream<Object>>> resultFutureB = streamB.executeAs(this.streamName);
		
		//A
		Stream<Stream<Object>> resultA = resultFutureA.get(1000, TimeUnit.MILLISECONDS);	
		List<Stream<Object>> resultStreamsA = resultA.collect(Collectors.toList());
		assertEquals(1, resultStreamsA.size());
		
		List<Object> partitionStreamsA = resultStreamsA.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsA.size());

		DStreamExecutionGraph contextA = (DStreamExecutionGraph) partitionStreamsA.get(0);
		assertEquals(2, contextA.getOperations().size());
		assertEquals("extract", contextA.getOperations().get(0).getLastOperationName());
		assertEquals("load", contextA.getOperations().get(1).getLastOperationName());
		resultA.close();
		
		//B
		Stream<Stream<Object>> resultB = resultFutureB.get(1000, TimeUnit.MILLISECONDS);	
		List<Stream<Object>> resultStreamsB = resultB.collect(Collectors.toList());
		assertEquals(1, resultStreamsB.size());
		
		List<Object> partitionStreamsB = resultStreamsB.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsB.size());

		DStreamExecutionGraph contextB = (DStreamExecutionGraph) partitionStreamsB.get(0);
		assertEquals(2, contextB.getOperations().size());
		assertEquals("flatMap", contextB.getOperations().get(0).getLastOperationName());
		assertEquals("load", contextB.getOperations().get(1).getLastOperationName());
		
		resultB.close();
	}
	
	@Test
	public void validateStreamJoin() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "validateStreamJoin");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream2<Object, Object> joinedStream = streamA.join(streamB).on(s -> true);
		Future<Stream<Stream<Tuple2<Object, Object>>>> resultFuture = joinedStream.executeAs(this.streamName);
		
		Stream<Stream<Tuple2<Object, Object>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Tuple2<Object, Object>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		DStreamExecutionGraph context = (DStreamExecutionGraph) partitionStreams.get(0);
		assertEquals(2, context.getOperations().size());
		assertEquals("extract", context.getOperations().get(0).getLastOperationName());
		assertEquals("join", context.getOperations().get(1).getLastOperationName());
		
		result.close();
	}
	
	@Test
	public void validateStreamJoinWithContinuation() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "validateStreamJoinWithContinuation");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream<Tuple2<Object, Object>> joinedStream = streamA.join(streamB).on(s -> true).map(s -> s);
		Future<Stream<Stream<Tuple2<Object, Object>>>> resultFuture = joinedStream.executeAs(this.streamName);
		
		Stream<Stream<Tuple2<Object, Object>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Tuple2<Object, Object>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());
		
		DStreamExecutionGraph context = (DStreamExecutionGraph) partitionStreams.get(0);
		assertEquals(2, context.getOperations().size());
		assertEquals("extract", context.getOperations().get(0).getLastOperationName());
		assertEquals("join", context.getOperations().get(1).getLastOperationName());
		
		result.close();
	}
	
	@Test
	public void validateGrouping() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "validatePartitioning");
		DStream<Object> partitioned = stream.classify(s -> s);
		
		Future<Stream<Stream<Object>>> resultFuture = partitioned.executeAs(this.streamName);
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Object>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		DStreamExecutionGraph context = (DStreamExecutionGraph) partitionStreams.get(0);
		assertEquals(2, context.getOperations().size());
		assertEquals("classify", context.getOperations().get(0).getLastOperationName());
		assertEquals("load", context.getOperations().get(1).getLastOperationName());
	}
	
	@Test
	public void validateGroupingAfterJoin() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "validatePartitioningAfterJoin");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream<Tuple2<Object, Object>> joinedStream = streamA.join(streamB).on(s -> true).map(s -> s);
		DStream<Entry<Tuple2<Object, Object>, List<Integer>>> partitionedReduced = joinedStream
				.classify(s -> s)
				.aggregateValues(s -> s, s -> 1);
		
		Future<Stream<Stream<Entry<Tuple2<Object, Object>, List<Integer>>>>> resultFuture = partitionedReduced.executeAs(this.streamName);
		Stream<Stream<Entry<Tuple2<Object, Object>, List<Integer>>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<Tuple2<Object, Object>, List<Integer>>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());
		
		DStreamExecutionGraph context = (DStreamExecutionGraph) partitionStreams.get(0);
		assertEquals(4, context.getOperations().size());
		assertEquals("extract", context.getOperations().get(0).getLastOperationName());
		assertEquals("classify", context.getOperations().get(1).getLastOperationName());
		assertEquals("mapKeyValues", context.getOperations().get(2).getLastOperationName());
		assertEquals("aggregateValues", context.getOperations().get(3).getLastOperationName());
	}
}
