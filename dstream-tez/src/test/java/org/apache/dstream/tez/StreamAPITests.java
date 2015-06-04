package org.apache.dstream.tez;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributableStream;
import org.junit.After;
import org.junit.Test;

public class StreamAPITests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void flatMapReduce() throws Exception {
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "wc");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduce(word -> word, word -> 1, Integer::sum)
			.executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(14, firstResult.size());
		Assert.assertEquals((Integer)2, firstResult.get(11).getValue());
		result.close();
	}
	
	@Test
	public void flatMapReduceMap() throws Exception {
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "wc");
		
		Future<Stream<Stream<String>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduce(word -> word, word -> 1, Integer::sum)
				.map(entry -> entry.toString())
			.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);
		
		List<String> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(14, firstResult.size());
		Assert.assertEquals("we=2", firstResult.get(11));
		
		result.close();
	}
	
	@Test
	public void flatMapFilterReduceMap() throws Exception {
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "wc");
		
		Future<Stream<Stream<String>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.filter(word -> word.equals("we"))
				.reduce(word -> word, word -> 1, Integer::sum)
				.map(entry -> entry.toString())
			.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);
		
		List<String> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(1, firstResult.size());
		Assert.assertEquals("we=2", firstResult.get(0));
		
		result.close();
	}
	
	@Test
	public void flatMapFilterMapReduceMap() throws Exception {
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "wc");
		
		Future<Stream<Stream<String>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.filter(word -> word.equals("we"))
				.map(word -> word.toUpperCase())
				.reduce(word -> word, word -> 1, Integer::sum)
				.map(entry -> entry.toString())
			.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);
		
		List<String> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(1, firstResult.size());
		Assert.assertEquals("WE=2", firstResult.get(0));
		
		result.close();
	}
}
