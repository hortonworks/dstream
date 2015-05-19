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
		
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "wc");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduce(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(20000, TimeUnit.MILLISECONDS);
		
		result.forEach(stream -> stream.forEach(System.out::println));
		result.close();
		
		BaseTezTests.clean("WordCount");
		System.exit(0);// until 0.6.1 Tez see TEZ-1661
	}

}
