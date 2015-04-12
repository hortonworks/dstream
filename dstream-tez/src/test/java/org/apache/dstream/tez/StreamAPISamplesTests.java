package org.apache.dstream.tez;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.UriSourceSupplier;
import org.junit.Test;

public class StreamAPISamplesTests {
	
	
	@Test
	public void filteredWordCount() {
		String applicationName = "WordCount";
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourceStream
			.flatMap(line -> Stream.of(line.split("\\s+")))
			.filter(word -> word.equals("we"))
			.reduce(word -> word, word -> 1, Integer::sum)
			.executeAs(applicationName);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
	}

}
