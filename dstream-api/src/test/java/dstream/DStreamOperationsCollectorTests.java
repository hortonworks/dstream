package dstream;

import static org.junit.Assert.assertEquals;

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
		assertEquals(2, context.getInvocations().size());
		assertEquals("join", context.getInvocations().get(0).getMethod().getName());
		
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
		assertEquals(3, context.getInvocations().size());
		assertEquals("join", context.getInvocations().get(0).getMethod().getName());
		assertEquals("on", context.getInvocations().get(1).getMethod().getName());
		assertEquals("map", context.getInvocations().get(2).getMethod().getName());
		
		result.close();
	}
	
	@Test
	public void validatePartitioning() throws Exception {
		DStream<Object> stream = DStream.ofType(Object.class, "validatePartitioning");
		DStream<Object> partitioned = stream.partition();
		
		Future<Stream<Stream<Object>>> resultFuture = partitioned.executeAs(this.streamName);
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Object>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());

		DStreamInvocationPipeline context = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(1, context.getInvocations().size());
		assertEquals("partition", context.getInvocations().get(0).getMethod().getName());
	}
	
	@Test
	public void validatePartitioningAfterJoin() throws Exception {
		DStream<Object> streamA = DStream.ofType(Object.class, "validatePartitioningAfterJoin");
		DStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		
		DStream<Tuple2<Object, Object>> joinedStream = streamA.join(streamB).on(s -> true).map(s -> s);
		DStream<Entry<Tuple2<Object, Object>, Integer>> partitionedReduced = joinedStream.partition().reduceGroups(s -> s, s -> 1, (a,b) -> a);
		
		Future<Stream<Stream<Entry<Tuple2<Object, Object>, Integer>>>> resultFuture = partitionedReduced.executeAs(this.streamName);
		Stream<Stream<Entry<Tuple2<Object, Object>, Integer>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<Tuple2<Object, Object>, Integer>>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		
		List<Object> partitionStreams = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreams.size());
		
		DStreamInvocationPipeline context = (DStreamInvocationPipeline) partitionStreams.get(0);
		assertEquals(5, context.getInvocations().size());
		assertEquals("join", context.getInvocations().get(0).getMethod().getName());
		assertEquals("on", context.getInvocations().get(1).getMethod().getName());
		assertEquals("map", context.getInvocations().get(2).getMethod().getName());
		assertEquals("partition", context.getInvocations().get(3).getMethod().getName());
		assertEquals("reduceGroups", context.getInvocations().get(4).getMethod().getName());
	}
}
