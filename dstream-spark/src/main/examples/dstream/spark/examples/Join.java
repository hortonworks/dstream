package dstream.spark.examples;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.dstream.DStream;
import io.dstream.utils.ExecutionResultUtils;
import io.dstream.utils.Tuples.Tuple3;

public class Join {

	public static void main(String... args) throws Exception {

		DStream<String> one = DStream.ofType(String.class, "one").classify(s -> s.split("\\s+")[0]);
		DStream<String> two = DStream.ofType(String.class, "two").classify(s -> s.split("\\s+")[2]);
		DStream<String> three = DStream.ofType(String.class, "three").classify(s -> s.split("\\s+")[0]);
		
		Future<Stream<Stream<Entry<String, List<Tuple3<String, String, String>>>>>> resultFuture = one
				.join(two).on(t2 -> t2._1().split("\\s+")[0].equals(t2._2().split("\\s+")[2]))
				.join(three).on(t3 -> t3._1().split("\\s+")[0].equals(t3._3().split("\\s+")[0]))
				.aggregateValues(t3 -> t3._1().split("\\s+")[0], t3 -> t3)
			.executeAs("Join");
		
		Stream<Stream<Entry<String, List<Tuple3<String, String, String>>>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
	}

}
