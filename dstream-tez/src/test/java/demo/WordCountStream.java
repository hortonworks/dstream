package demo;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.UriSourceSupplier;

/**
 * A rudimentary WordCount
 *
 */
public class WordCountStream {

	public static void main(String[] args) {
		
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
		Stream<Stream<Entry<String, Integer>>> result = sourceStream
			.flatMap(line -> Stream.of(line.split("\\s+")))
			.reduce(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
		result.forEach(stream -> stream.forEach(System.out::println));
		
		result.close();
		System.exit(0);// until 0.6.1 Tez see TEZ-1661
	}

}
