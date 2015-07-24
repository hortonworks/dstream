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
import org.junit.After;
import org.junit.Test;

public class GroupTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void streamGroup() throws Exception {	
		DStream<String> sourceStream = DStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, Iterable<Integer>>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))	
				.group(s -> s, s -> 1)
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, Iterable<Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Iterable<Integer>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Iterable<Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Iterable<Integer>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		
		Entry<String, Iterable<Integer>> e1 = firstResult.get(0);
		assertEquals("bar", e1.getKey());
		assertEquals(3, StreamSupport.stream(e1.getValue().spliterator(), false).count());
		
		Entry<String, Iterable<Integer>> e2 = firstResult.get(1);
		assertEquals("baz", e2.getKey());
		assertEquals(2, StreamSupport.stream(e2.getValue().spliterator(), false).count());
		
		Entry<String, Iterable<Integer>> e4 = firstResult.get(3);
		assertEquals("dang", e4.getKey());
		assertEquals(1, StreamSupport.stream(e4.getValue().spliterator(), false).count());
		
		result.close();
	}

	@Test
	public void streamGroupPreExistingKV() throws Exception {	
		DStream<String> sourceStream = DStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, Iterable<Integer>>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.group(s -> s.getKey(), s -> s.getValue())
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, Iterable<Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Iterable<Integer>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Iterable<Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Iterable<Integer>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		
		Entry<String, Iterable<Integer>> e1 = firstResult.get(0);
		assertEquals("bar", e1.getKey());
		assertEquals(3, StreamSupport.stream(e1.getValue().spliterator(), false).count());
		
		Entry<String, Iterable<Integer>> e2 = firstResult.get(1);
		assertEquals("baz", e2.getKey());
		assertEquals(2, StreamSupport.stream(e2.getValue().spliterator(), false).count());
		
		Entry<String, Iterable<Integer>> e4 = firstResult.get(3);
		assertEquals("dang", e4.getKey());
		assertEquals(1, StreamSupport.stream(e4.getValue().spliterator(), false).count());
		
		result.close();
	}
	
}
