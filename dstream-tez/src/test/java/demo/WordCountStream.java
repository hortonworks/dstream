package demo;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.tez.BaseTezTests;

/**
 * A rudimentary WordCount
 *
 */
public class WordCountStream {

	public static void main(String[] args) throws Exception {
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DistributableStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.combine(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
		// each stream within a stream represents a partition essentially giving you access to each result partition
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		result.close();
		
		BaseTezTests.clean("WordCount");
		System.exit(0);// until 0.6.1 Tez see TEZ-1661
	}

}
