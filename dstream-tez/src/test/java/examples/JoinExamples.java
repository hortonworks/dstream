package examples;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.DStream;
import org.apache.dstream.support.Aggregators;
import org.apache.dstream.tez.BaseTezTests;
import org.apache.dstream.utils.Tuples.Tuple2;

public class JoinExamples {
	
	static String EXECUTION_NAME = "JoinExamples";
	
	public static void main(String[] args) throws Exception {
		//run all
		TwoWayCrossJoin.main();
		TwoWayJoinWithPredicate.main();
		TwoWayJoinWithPredicateAndReduce.main();
		TwoWayJoinWithPredicateAndGroup.main();
		BaseTezTests.clean(EXECUTION_NAME);
	}
	
	public static class TwoWayCrossJoin {
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> hash = DStream.ofType(String.class, "hash");
			DStream<String> probe = DStream.ofType(String.class, "probe");
			
			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash
					.join(probe)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class TwoWayJoinWithPredicate {
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> hash = DStream.ofType(String.class, "hash");
			DStream<String> probe = DStream.ofType(String.class, "probe");
			
			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash
					.join(probe).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class TwoWayJoinWithPredicateAndReduce {
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> hash = DStream.ofType(String.class, "hash");
			DStream<String> probe = DStream.ofType(String.class, "probe");
			
			Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = hash
					.join(probe).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.reduceGroups(s -> s._1(), s -> 1, Integer::sum)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Entry<String, Integer>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class TwoWayJoinWithPredicateAndGroup {
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> hash = DStream.ofType(String.class, "hash");
			DStream<String> probe = DStream.ofType(String.class, "probe");
			
			Future<Stream<Stream<Entry<String, List<String>>>>> resultFuture = hash
					.join(probe).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.aggregateGroups(s -> s._1(), s -> s._2(), Aggregators::aggregateFlatten)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Entry<String, List<String>>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
}
