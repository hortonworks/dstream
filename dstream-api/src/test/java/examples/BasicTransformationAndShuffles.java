package examples;
import static dstream.utils.Tuples.Tuple2.tuple2;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.utils.Tuples.Tuple2;

/**
 * A rudimentary WordCount
 *
 */
public class BasicTransformationAndShuffles {
	
	static String EXECUTION_NAME = "BasicTransformationAndShuffles";

	public static void main(String[] args) throws Exception {
		//run all
		NoTransformationNoShuffles.main();
		TransformationNoShuffles.main();
		TransformationAndReduceShuffle.main();
		TransformationAndAggregateShuffle.main(args);
		CombinationOfTransformationAndShuffles.main();
	}
	
	public static class NoTransformationNoShuffles extends SampleBase {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.executeAs(EXECUTION_NAME);

			Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
			printResults(resultPartitionsStream);
		}
	}
	
	public static class TransformationNoShuffles extends SampleBase {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(s -> s.toUpperCase())
				.executeAs(EXECUTION_NAME);

			Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
			printResults(resultPartitionsStream);
		}
	}
	
	public static class TransformationAndReduceShuffle extends SampleBase {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.reduceValues(word -> word, word -> 1, Integer::sum)
				.executeAs(EXECUTION_NAME);
			
			// each stream within a stream represents a partition essentially giving you access to each result partition
			Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
			printResults(resultPartitionsStream);
		}
	}
	
	public static class TransformationAndAggregateShuffle extends SampleBase {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<Entry<Integer, List<String>>>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.aggregateValues(word -> word.length(), word -> word)
				.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Entry<Integer, List<String>>>> resultPartitionsStream = resultFuture.get();
			printResults(resultPartitionsStream);
		}
	}
	
	public static class CombinationOfTransformationAndShuffles extends SampleBase {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<Entry<Integer, List<Tuple2<String, Integer>>>>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.filter(word -> word.length() > 4)
					.reduceValues(word -> word, word -> 1, Integer::sum)
					.map(entry -> tuple2(entry.getKey(), entry.getValue()))
					.aggregateValues(s -> s._1().length(), s -> s)
				.executeAs(EXECUTION_NAME);
			
			// each stream within a stream represents a partition essentially giving you access to each result partition
			Stream<Stream<Entry<Integer, List<Tuple2<String, Integer>>>>> resultPartitionsStream = resultFuture.get();
			printResults(resultPartitionsStream);
		}
	}
}
