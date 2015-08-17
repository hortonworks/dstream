package dstream.examples;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.DStream.DStream2;
import dstream.utils.ExecutionResultUtils;
import dstream.utils.KVUtils;
import dstream.utils.Tuples.Tuple2;
import dstream.utils.Tuples.Tuple3;

public class Join {
	
	static String EXECUTION_NAME = "Join";
	
	public static void main(String[] args) throws Exception {
		//run all
//		TwoWayCrossJoin.main();
//		TwoWayJoinWithPredicate.main();
	}
	
//	public static class TwoWayCrossJoin {
//		public static void main(String... args) throws Exception {
//			DStream<String> one = DStream.ofType(String.class, "one");
//			DStream<String> two = DStream.ofType(String.class, "two");
//			
//			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
//					.join(two)
//				.executeAs(EXECUTION_NAME);
//			
//			Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
//			ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		}
//	}
//	
//	public static class ThreeWayCrossJoin {
//		public static void main(String... args) throws Exception {
//			DStream<String> one = DStream.ofType(String.class, "one");
//			DStream<String> two = DStream.ofType(String.class, "two");
//			DStream<String> three = DStream.ofType(String.class, "three");
//			
//			Future<Stream<Stream<Tuple3<String, String, String>>>> resultFuture = one
//					.join(two)
//					.join(three)
//				.executeAs(EXECUTION_NAME);
//			
//			Stream<Stream<Tuple3<String, String, String>>> resultPartitionsStream = resultFuture.get();
//			ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		}
//	}
	
	public static class TwoWayJoinWithPredicate {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			DStream<String> three = DStream.ofType(String.class, "three");
			
//			one.group(s -> s)
//			.join(two.group(s -> s)).on(s -> true)
//			.map(s -> s).r;
		
			
//			Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
//			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
}
