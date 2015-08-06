package org.apache.dstream.tez;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DStream;
import org.apache.dstream.utils.KVUtils;
import org.junit.After;
import org.junit.Test;

public class StreamAPIUnionTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void unionDistinct() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
				.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);

		List<String> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(4, firstResult.size());

		assertEquals(firstResult.get(0).trim(), "You have to learn the rules of the game.");
		assertEquals(firstResult.get(1).trim(), "And then you have to play better than anyone else.");
		assertEquals(firstResult.get(2).trim(), "Look deep into nature, and then you will");
		assertEquals(firstResult.get(3).trim(), "understand everything better.");
		
		result.close();
	}
	
	@Test
	public void unionDistinctThreeWay() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		DStream<String> three = DStream.ofType(String.class, "three");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two).union(three)
				.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);

		List<String> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(4, firstResult.size());

		assertEquals(firstResult.get(0).trim(), "You have to learn the rules of the game.");
		assertEquals(firstResult.get(1).trim(), "And then you have to play better than anyone else.");
		assertEquals(firstResult.get(2).trim(), "Look deep into nature, and then you will");
		assertEquals(firstResult.get(3).trim(), "understand everything better.");
		
		result.close();
	}
	
	@Test
	public void unionDistinctThreeWayWithTransformationInBetween() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		DStream<String> three = DStream.ofType(String.class, "three");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
				.map(line -> line.toUpperCase())
				.union(three)
				.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);

		List<String> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(8, firstResult.size());

		assertEquals(firstResult.get(0).trim(), "YOU HAVE TO LEARN THE RULES OF THE GAME.");
		assertEquals(firstResult.get(1).trim(), "AND THEN YOU HAVE TO PLAY BETTER THAN ANYONE ELSE.");
		assertEquals(firstResult.get(2).trim(), "LOOK DEEP INTO NATURE, AND THEN YOU WILL");
		assertEquals(firstResult.get(3).trim(), "UNDERSTAND EVERYTHING BETTER.");
		assertEquals(firstResult.get(4).trim(), "You have to learn the rules of the game.");
		assertEquals(firstResult.get(5).trim(), "And then you have to play better than anyone else.");
		assertEquals(firstResult.get(6).trim(), "Look deep into nature, and then you will");
		assertEquals(firstResult.get(7).trim(), "understand everything better.");
		
		result.close();
	}
	
	@Test
	public void unionDistinctThreeWayWithShuffleInBetween() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		DStream<String> three = DStream.ofType(String.class, "three");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = one
				.union(two)
				.map(line -> line.toUpperCase())
				.reduceGroups(s -> s, s -> 1, Integer::sum)
				.union(three.map(s -> KVUtils.kv(s.toUpperCase(), 1)))
				.executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);

		List<Entry<String, Integer>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(4, firstResult.size());

		assertEquals(firstResult.get(0).toString(), "AND THEN YOU HAVE TO PLAY BETTER THAN ANYONE ELSE.=1");
		assertEquals(firstResult.get(1).toString(), "LOOK DEEP INTO NATURE, AND THEN YOU WILL =1");
		assertEquals(firstResult.get(2).toString(), "UNDERSTAND EVERYTHING BETTER.=1");
		assertEquals(firstResult.get(3).toString(), "YOU HAVE TO LEARN THE RULES OF THE GAME. =1");
		result.close();
	}
	
	@Test
	public void unionAll() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.unionAll(two)
				.executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);

		List<String> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(6, firstResult.size());

		assertEquals(firstResult.get(0).trim(), "You have to learn the rules of the game.");
		assertEquals(firstResult.get(1).trim(), "And then you have to play better than anyone else.");
		assertEquals(firstResult.get(2).trim(), "You have to learn the rules of the game.");
		assertEquals(firstResult.get(3).trim(), "And then you have to play better than anyone else.");
		assertEquals(firstResult.get(4).trim(), "Look deep into nature, and then you will");
		assertEquals(firstResult.get(5).trim(), "understand everything better.");
		
		result.close();
	}
	
}
