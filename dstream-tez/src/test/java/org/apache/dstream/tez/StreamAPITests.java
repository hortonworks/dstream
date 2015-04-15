package org.apache.dstream.tez;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.UriSourceSupplier;
import org.junit.Test;

public class StreamAPITests {
	
	
	@Test
	public void flatMapReduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduce(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(14, firstResult.size());
		Assert.assertEquals((Integer)2, firstResult.get(11).getValue());
		result.close();
	}
	
	@Test
	public void flatMapReduceMap() {
		String applicationName = "WordCount";
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
		Stream<Stream<String>> result = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduce(word -> word, word -> 1, Integer::sum)
				.map(entry -> entry.toString())
			.executeAs(applicationName);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);
		
		List<String> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(14, firstResult.size());
		Assert.assertEquals("we=2", firstResult.get(11));
		
		result.close();
	}
	
	@Test
	public void flatMapFilterReduceMap() {
		String applicationName = "WordCount";
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
		Stream<Stream<String>> result = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.filter(word -> word.equals("we"))
				.reduce(word -> word, word -> 1, Integer::sum)
				.map(entry -> entry.toString())
			.executeAs(applicationName);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);
		
		List<String> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(1, firstResult.size());
		Assert.assertEquals("we=2", firstResult.get(0));
		
		result.close();
	}
	
	@Test
	public void flatMapFilterMapReduceMap() {
		String applicationName = "WordCount";
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
		Stream<Stream<String>> result = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.filter(word -> word.equals("we"))
				.map(word -> word.toUpperCase())
				.reduce(word -> word, word -> 1, Integer::sum)
				.map(entry -> entry.toString())
			.executeAs(applicationName);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<String> firstResultStream = resultStreams.get(0);
		
		List<String> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(1, firstResult.size());
		Assert.assertEquals("WE=2", firstResult.get(0));
		
		result.close();
	}
}
