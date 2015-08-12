package dstream.examples;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.utils.ExecutionResultUtils;

/**
 * A quintessential WordCount
 *
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduceValues(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
		// each stream within a stream represents a partition essentially giving you access to each result partition
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		resultPartitionsStream.close();
	}
}
