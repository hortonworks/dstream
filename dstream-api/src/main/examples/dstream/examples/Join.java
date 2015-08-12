package dstream.examples;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.utils.ExecutionResultUtils;
import dstream.utils.Tuples.Tuple2;

public class Join {
	
	static String EXECUTION_NAME = "Join";
	
	public static void main(String[] args) throws Exception {
		//run all
		TwoWayCrossJoin.main();
		TwoWayJoinWithPredicate.main();
	}
	
	public static class TwoWayCrossJoin {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
					.join(two)
				.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
	
	public static class TwoWayJoinWithPredicate {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
					.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
}
