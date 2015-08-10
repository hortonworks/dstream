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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import dstream.DStream;
import dstream.DStreamInvocationPipeline;
import dstream.DStream.DStream2;
import dstream.utils.Tuples.Tuple2;

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
		assertEquals("validateConstruction", pipeline.getSourceIdentifier());
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

		DStreamInvocationPipeline chainAccessor = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(0, chainAccessor.getInvocations().size());
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

		DStreamInvocationPipeline contextA = (DStreamInvocationPipeline) partitionStreamsA.get(0);
		assertEquals(0, contextA.getInvocations().size());
		
		resultA.close();
		
		//B
		Stream<Stream<Object>> resultB = resultFutureB.get(1000, TimeUnit.MILLISECONDS);	
		List<Stream<Object>> resultStreamsB = resultB.collect(Collectors.toList());
		assertEquals(1, resultStreamsB.size());
		
		List<Object> partitionStreamsB = resultStreamsB.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsB.size());

		DStreamInvocationPipeline contextB = (DStreamInvocationPipeline) partitionStreamsB.get(0);
		assertEquals(3, contextB.getInvocations().size());
		
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

		DStreamInvocationPipeline context = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(1, context.getInvocations().size());
		assertEquals("join", context.getInvocations().get(0).getMethod().getName());
		assertNotNull(context.getInvocations().get(0).getSupplementaryOperation());
		
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
		
		DStreamInvocationPipeline context = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(2, context.getInvocations().size());
		assertEquals("join", context.getInvocations().get(0).getMethod().getName());
		assertEquals("map", context.getInvocations().get(1).getMethod().getName());
		assertNotNull(context.getInvocations().get(0).getSupplementaryOperation());
		
		result.close();
	}
	
	@Test
	public void validateGrouping() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "validatePartitioning");
		DStream<Object> partitioned = stream.group(s -> s);
		
		Future<Stream<Stream<Object>>> resultFuture = partitioned.executeAs(this.streamName);
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Object>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		DStreamInvocationPipeline context = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(1, context.getInvocations().size());
		assertEquals("group", context.getInvocations().get(0).getMethod().getName());
	}
	
	@Test
	public void validateGroupingAfterJoin() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "validatePartitioningAfterJoin");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream<Tuple2<Object, Object>> joinedStream = streamA.join(streamB).on(s -> true).map(s -> s);
		DStream<Entry<Tuple2<Object, Object>, List<Integer>>> partitionedReduced = joinedStream
				.group(s -> s)
				.aggregateValues(s -> s, s -> 1);
		
		Future<Stream<Stream<Entry<Tuple2<Object, Object>, List<Integer>>>>> resultFuture = partitionedReduced.executeAs(this.streamName);
		Stream<Stream<Entry<Tuple2<Object, Object>, List<Integer>>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<Tuple2<Object, Object>, List<Integer>>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());
		
		DStreamInvocationPipeline context = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(4, context.getInvocations().size());
		assertEquals("join", context.getInvocations().get(0).getMethod().getName());
		assertNotNull(context.getInvocations().get(0).getSupplementaryOperation());
		assertEquals("map", context.getInvocations().get(1).getMethod().getName());
		assertEquals("group", context.getInvocations().get(2).getMethod().getName());
		assertEquals("aggregateValues", context.getInvocations().get(3).getMethod().getName());
	}
}
