package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.UriSourceSupplier;
import org.junit.Test;

public class PipelineAPITests {
	
	private final String applicationName = "WordCount";
	
	@Test
	public void computeReduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			).reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			 .executeAs(this.applicationName);
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(14, firstResult.size());
		Assert.assertEquals((Integer)2, firstResult.get(11).getValue());
		
		result.close();
	}
	
	@Test
	public void computeReduceCompute() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			).reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			 .<Entry<String, Integer>>compute(stream -> stream
				.map(entry -> kv(entry.getKey(), entry.getValue()))
				.filter(entry -> entry.getKey().equals("we"))
			).executeAs(this.applicationName);
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(1, firstResult.size());
		Assert.assertEquals("we", firstResult.get(0).getKey());
		Assert.assertEquals((Integer)2, firstResult.get(0).getValue());
		
		result.close();
	}
	
	@Test
	public void computeReduceComputeNonEntry() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<String>> result = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			).reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			 .<String>compute(stream -> stream
				.map(entry -> kv(entry.getKey(), entry.getValue()))
				.filter(entry -> entry.getKey().equals("we"))
				.map(entry -> entry.toString())
			).executeAs(this.applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}
	
	@Test
	public void reduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<Integer, Integer>>> result = sourcePipeline
				.reduce(s -> s.length(), s -> 1, Integer::sum)
				.executeAs(this.applicationName);
		
		List<Stream<Entry<Integer, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<Integer, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<Integer, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		Assert.assertEquals((Integer)16, firstResult.get(0).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(0).getValue());
		
		Assert.assertEquals((Integer)31, firstResult.get(1).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(1).getValue());
			
		Assert.assertEquals((Integer)34, firstResult.get(2).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(2).getValue());
		
		result.close();
	}
	
	@Test
	public void compute() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = 
				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(word -> kv(word, 1))
				).executeAs(this.applicationName);
		
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(15, firstResult.size());
		
		Assert.assertEquals("we", firstResult.get(9).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(9).getValue());
		
		Assert.assertEquals("we", firstResult.get(12).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(12).getValue());
		
		result.close();
	}
	
	@Test
	public void computeCompute() {  
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = 
				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(word -> kv(word, 1))
				)
				.compute(stream -> stream.filter(s -> s.getKey().length() == 2))
				.executeAs(this.applicationName);
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		Assert.assertEquals("We", firstResult.get(0).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(0).getValue());
		
		Assert.assertEquals("we", firstResult.get(1).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(1).getValue());
		
		Assert.assertEquals("we", firstResult.get(2).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(2).getValue());
		
		result.close();
	}
	
	@Test
	public void computeComputeComputeReduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = 
				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(word -> kv(word, 1))
				)
				.compute(stream -> stream.filter(s -> s.getKey().startsWith("we")))
				.compute(stream -> stream.filter(s -> s.getKey().length() < 3))
				.reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
				.executeAs(this.applicationName);
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(1, firstResult.size());
		
		Assert.assertEquals("we", firstResult.get(0).getKey());
		Assert.assertEquals((Integer)2, firstResult.get(0).getValue());
		
		result.close();
	}
	
	@Test
	public void computeReduceComputeComputeReduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = 
				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(word -> kv(word, 1))
				).reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
				 .<Entry<String, Integer>>compute(stream -> stream
					.map(entry -> kv(entry.getKey().toUpperCase(), entry.getValue()))
				)
				.compute(stream -> stream.filter(entry -> entry.getKey().startsWith("W")))
				 .reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
				.executeAs(this.applicationName);
		
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		Assert.assertEquals("WE", firstResult.get(0).getKey());
		Assert.assertEquals((Integer)3, firstResult.get(0).getValue());
		
		Assert.assertEquals("WHEN", firstResult.get(1).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(1).getValue());
		
		Assert.assertEquals("WITH", firstResult.get(2).getKey());
		Assert.assertEquals((Integer)1, firstResult.get(2).getValue());
		
		result.close();
	}
	
	@Test
	public void reduceReduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Long>>> result = sourcePipeline
				.reduce(s -> s.toUpperCase(), s -> 1, Integer::sum)
				.reduce(s -> s.getKey().trim() + "_" + s.hashCode(), s -> 1L, Long::sum)
				.executeAs(this.applicationName);
		
		List<Stream<Entry<String, Long>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Long>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Long>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		System.out.println(firstResult);
		
		Assert.assertEquals("THE SAME THINKING WE USED WHEN", firstResult.get(0).getKey().trim().split("_")[0]);
		Assert.assertEquals((Long)1L, firstResult.get(0).getValue());
		
		result.close();
	}

}
