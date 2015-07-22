package org.apache.dstream;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.StreamOperationsCollector.ProxyInternalsAccessor;
import org.junit.Test;

public class StreamOperationsCollectorTests {

	private String pipelineName = StreamOperationsCollectorTests.class.getSimpleName();
	
	@Test(expected=IllegalArgumentException.class)
	public void failNullElementType() throws Exception {
		DistributableStream.ofType(null, "foo");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void failNoName() throws Exception {
		DistributableStream.ofType(Object.class, "");
	}
	
	@Test
	public void validateConstruction() throws Exception {
		DistributableStream<Object> pipeline = DistributableStream.ofType(Object.class, "foo");
		assertEquals("foo", pipeline.getSourceIdentifier());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateRawStreamExecution() throws Exception {
		DistributableStream<Object> stream = DistributableStream.ofType(Object.class, "foo");
		Future<Stream<Stream<Object>>> resultFuture = stream.executeAs(this.pipelineName);
		
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
		DistributableStream<Object> streamA = DistributableStream.ofType(Object.class, "foo");
		DistributableStream<Object> streamB = streamA.filter(s -> true).map(s -> s).flatMap(s -> Stream.of(s));
		Future<Stream<Stream<Object>>> resultFutureA = streamA.executeAs(this.pipelineName);
		Future<Stream<Stream<Object>>> resultFutureB = streamB.executeAs(this.pipelineName);
		
		//A
		Stream<Stream<Object>> resultA = resultFutureA.get();	
		List<Stream<Object>> resultStreamsA = resultA.collect(Collectors.toList());
		assertEquals(1, resultStreamsA.size());
		
		List<Object> partitionStreamsA = resultStreamsA.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsA.size());

		ProxyInternalsAccessor<StreamInvocationChain> contextA = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreamsA.get(0);
		assertEquals(0, contextA.get().getInvocations().size());
		
		resultA.close();
		
		//B
		Stream<Stream<Object>> resultB = resultFutureB.get();	
		List<Stream<Object>> resultStreamsB = resultB.collect(Collectors.toList());
		assertEquals(1, resultStreamsB.size());
		
		List<Object> partitionStreamsB = resultStreamsB.get(0).collect(Collectors.toList());
		assertEquals(1, partitionStreamsB.size());

		ProxyInternalsAccessor<StreamInvocationChain> contextB = (ProxyInternalsAccessor<StreamInvocationChain>) partitionStreamsB.get(0);
		assertEquals(3, contextB.get().getInvocations().size());
		
		resultB.close();
	}
}
