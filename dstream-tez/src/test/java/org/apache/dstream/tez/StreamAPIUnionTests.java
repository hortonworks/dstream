package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;
import static org.apache.dstream.utils.Tuples.Tuple2.tuple2;
import static org.apache.dstream.utils.Tuples.Tuple3.tuple3;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DStream;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.apache.dstream.utils.Tuples.Tuple3;
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
