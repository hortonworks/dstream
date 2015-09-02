package sample.standalone;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.utils.ExecutionResultUtils;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
			.flatMap(record -> Stream.of(record.split("\\s+")))
			.reduceValues(word -> word, word -> 1, Integer::sum)
		.executeAs("WordCount");
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		ExecutionResultUtils.printResults(resultPartitionsStream);
		
	}

}
