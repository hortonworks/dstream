package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.UriSourceSupplier;
import org.junit.Test;

public class PipelineAPISamplesTests {
	
	private final String applicationName = "WordCount";
	
	@Test
	public void classicWordCount() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			)
			.reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			.executeAs(this.applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}
	
	@Test
	public void implicitMapperAndReducer() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<Integer, Integer>>> result = sourcePipeline
				.reduce(s -> s.length(), s -> 1, Integer::sum)
				.executeAs(this.applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}
	
	@Test
	public void singleMapper() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = 
				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(word -> kv(word, 1))
				).executeAs(this.applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}
	
	@Test
	public void twoComputeStages() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = 
				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(word -> kv(word, 1))
				)
				.compute(stream -> stream.filter(s -> s.getKey().startsWith("we")))
				.executeAs(this.applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}
	
	@Test
	public void reduceReduce() {
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/tez/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourcePipeline
				.reduce(s -> s.hashCode(), s -> 1, Integer::sum)
				.reduce(s -> "VAL", s -> 1, Integer::sum)
				.executeAs(this.applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}

}
