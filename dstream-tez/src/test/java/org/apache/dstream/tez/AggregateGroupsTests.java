package org.apache.dstream.tez;

import static org.junit.Assert.assertEquals;
import static org.apache.dstream.utils.KVUtils.kv;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import junit.framework.Assert;

import org.apache.dstream.DStream;
import org.apache.dstream.support.Aggregators;
import org.junit.After;
import org.junit.Test;

public class AggregateGroupsTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void defaultAggregator() throws Exception {	
		DStream<String> sourceStream = DStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, List<Integer>>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))	
				.aggregateGroups(s -> s, s -> 1)
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, List<Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, List<Integer>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, List<Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, List<Integer>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		
		Entry<String, List<Integer>> e1 = firstResult.get(0);
		assertEquals("bar", e1.getKey());
		assertEquals(3, StreamSupport.stream(e1.getValue().spliterator(), false).count());
		
		Entry<String, List<Integer>> e2 = firstResult.get(1);
		assertEquals("baz", e2.getKey());
		assertEquals(2, StreamSupport.stream(e2.getValue().spliterator(), false).count());
		
		Entry<String, List<Integer>> e4 = firstResult.get(3);
		assertEquals("dang", e4.getKey());
		assertEquals(1, StreamSupport.stream(e4.getValue().spliterator(), false).count());
		
		result.close();
	}
	
	@Test
	public void providedAggregator() throws Exception {	
		DStream<String> sourceStream = DStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, List<Integer>>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))	
				.aggregateGroups(s -> s, s -> 1, Aggregators::aggregateFlatten)
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, List<Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, List<Integer>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, List<Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, List<Integer>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		
		Entry<String, List<Integer>> e1 = firstResult.get(0);
		assertEquals("bar", e1.getKey());
		assertEquals(3, StreamSupport.stream(e1.getValue().spliterator(), false).count());
		
		Entry<String, List<Integer>> e2 = firstResult.get(1);
		assertEquals("baz", e2.getKey());
		assertEquals(2, StreamSupport.stream(e2.getValue().spliterator(), false).count());
		
		Entry<String, List<Integer>> e4 = firstResult.get(3);
		assertEquals("dang", e4.getKey());
		assertEquals(1, StreamSupport.stream(e4.getValue().spliterator(), false).count());
		
		result.close();
	}

	@Test
	public void aggregateWithPreExistingKV() throws Exception {	
		DStream<String> sourceStream = DStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, List<Integer>>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.aggregateGroups(s -> s.getKey(), s -> s.getValue(), Aggregators::aggregateFlatten)
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, List<Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, List<Integer>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, List<Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, List<Integer>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		
		Entry<String, List<Integer>> e1 = firstResult.get(0);
		assertEquals("bar", e1.getKey());
		assertEquals(3, StreamSupport.stream(e1.getValue().spliterator(), false).count());
		
		Entry<String, List<Integer>> e2 = firstResult.get(1);
		assertEquals("baz", e2.getKey());
		assertEquals(2, StreamSupport.stream(e2.getValue().spliterator(), false).count());
		
		Entry<String, List<Integer>> e4 = firstResult.get(3);
		assertEquals("dang", e4.getKey());
		assertEquals(1, StreamSupport.stream(e4.getValue().spliterator(), false).count());
		
		result.close();
	}
	
}
