package examples;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.DStream;
import org.apache.dstream.tez.BaseTezTests;

/**
 * A rudimentary WordCount
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduceGroups(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
		// each stream within a stream represents a partition essentially giving you access to each result partition
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get();
		result.forEach(resultPartitionStream -> {
			resultPartitionStream.forEach(System.out::println);
		});
		result.close();
		BaseTezTests.clean("WordCount");
	}
}
