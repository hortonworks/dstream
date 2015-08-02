package org.apache.dstream;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DStream.DStream2;
import org.apache.dstream.DStreamOperationsCollector.ProxyInternalsAccessor;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.junit.Test;

public class DStreamOperationsCollectorTests {

	private String streamName = DStreamOperationsCollectorTests.class.getSimpleName();
	
	@Test(expected=IllegalArgumentException.class)
	public void failNullElementType() throws Exception {
		DStream.ofType(null, "foo");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void failNoName() throws Exception {
		DStream.ofType(Object.class, "");
	}
	
	@Test
	public void validateConstruction() throws Exception {
		DStream<Object> pipeline = DStream.ofType(Object.class, "foo");
		assertEquals("foo", pipeline.getSourceIdentifier());
	}
	
	@Test
	public void validateToString() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "foo").filter(s -> true).map(s -> s);
		assertEquals("foo:[filter, map]", stream.toString());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateRawStreamExecution() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "foo");
		Future<Stream<Stream<Object>>> resultFuture = stream.executeAs(this.streamName);
		
		Stream<Stream<Object>> result = resultFuture.get();	
		List<Stream<Object>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		ProxyInternalsAccessor<StreamInvocationChain> chainAccessor = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreams.get(0);
		assertEquals(0, chainAccessor.get().getInvocations().size());
		result.close();
	}
	

	@SuppressWarnings("unchecked")
	@Test
	public void validateStreamInstanceIndependence() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "foo");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		Future<Stream<Stream<Object>>> resultFutureA = streamA.executeAs(this.streamName);
		Future<Stream<Stream<Object>>> resultFutureB = streamB.executeAs(this.streamName);
		
		//A
		Stream<Stream<Object>> resultA = resultFutureA.get(1000, TimeUnit.MILLISECONDS);	
		List<Stream<Object>> resultStreamsA = resultA.collect(Collectors.toList());
		assertEquals(1, resultStreamsA.size());
		
		List<Object> partitionStreamsA = resultStreamsA.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsA.size());

		ProxyInternalsAccessor<StreamInvocationChain> contextA = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreamsA.get(0);
		assertEquals(0, contextA.get().getInvocations().size());
		
		resultA.close();
		
		//B
		Stream<Stream<Object>> resultB = resultFutureB.get(1000, TimeUnit.MILLISECONDS);	
		List<Stream<Object>> resultStreamsB = resultB.collect(Collectors.toList());
		assertEquals(1, resultStreamsB.size());
		
		List<Object> partitionStreamsB = resultStreamsB.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsB.size());

		ProxyInternalsAccessor<StreamInvocationChain> contextB = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreamsB.get(0);
		assertEquals(3, contextB.get().getInvocations().size());
		
		resultB.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateStreamJoin() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "foo");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream2<Object, Object> joinedStream = streamA.join(streamB).on(s -> true);
		Future<Stream<Stream<Tuple2<Object, Object>>>> resultFuture = joinedStream.executeAs(this.streamName);
		
		Stream<Stream<Tuple2<Object, Object>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Tuple2<Object, Object>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		ProxyInternalsAccessor<StreamInvocationChain> context = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreams.get(0);
		assertEquals(2, context.get().getInvocations().size());
		assertEquals("join", context.get().getInvocations().get(0).getMethod().getName());
		
		result.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateStreamJoinWithContinuation() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "foo");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream<Tuple2<Object, Object>> joinedStream = streamA.join(streamB).on(s -> true).map(s -> s);
		Future<Stream<Stream<Tuple2<Object, Object>>>> resultFuture = joinedStream.executeAs(this.streamName);
		
		Stream<Stream<Tuple2<Object, Object>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Tuple2<Object, Object>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		ProxyInternalsAccessor<StreamInvocationChain> context = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreams.get(0);
		assertEquals(3, context.get().getInvocations().size());
		assertEquals("join", context.get().getInvocations().get(0).getMethod().getName());
		assertEquals("on", context.get().getInvocations().get(1).getMethod().getName());
		assertEquals("map", context.get().getInvocations().get(2).getMethod().getName());
		
		result.close();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validatePartitioning() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "foo");
		DStream<Object> partitioned = stream.partition();
		
		Future<Stream<Stream<Object>>> resultFuture = partitioned.executeAs(this.streamName);
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Object>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		ProxyInternalsAccessor<StreamInvocationChain> context = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreams.get(0);
		assertEquals(1, context.get().getInvocations().size());
		assertEquals("partition", context.get().getInvocations().get(0).getMethod().getName());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validatePartitioningAfterJoin() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "foo");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream<Tuple2<Object, Object>> joinedStream = streamA.join(streamB).on(s -> true).map(s -> s);
		DStream<Entry<Tuple2<Object, Object>, Integer>> partitionedReduced = joinedStream.partition().reduceGroups(s -> s, s -> 1, (a,b) -> a);
		
		Future<Stream<Stream<Entry<Tuple2<Object, Object>, Integer>>>> resultFuture = partitionedReduced.executeAs(this.streamName);
		Stream<Stream<Entry<Tuple2<Object, Object>, Integer>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<Tuple2<Object, Object>, Integer>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Entry<Tuple2<Object, Object>, Integer>> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());
		
		ProxyInternalsAccessor<StreamInvocationChain> context = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreams.get(0);
		assertEquals(5, context.get().getInvocations().size());
		assertEquals("join", context.get().getInvocations().get(0).getMethod().getName());
		assertEquals("on", context.get().getInvocations().get(1).getMethod().getName());
		assertEquals("map", context.get().getInvocations().get(2).getMethod().getName());
		assertEquals("partition", context.get().getInvocations().get(3).getMethod().getName());
		assertEquals("reduceGroups", context.get().getInvocations().get(4).getMethod().getName());
	}
}
