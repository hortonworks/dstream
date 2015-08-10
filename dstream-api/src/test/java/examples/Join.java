package examples;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.support.Aggregators;
import dstream.utils.Tuples.Tuple2;

public class Join {
	
	static String EXECUTION_NAME = "Join";
	
	public static void main(String[] args) throws Exception {
		//run all
		TwoWayCrossJoin.main();
		TwoWayJoinWithPredicate.main();
		TwoWayJoinWithPredicateAndReduce.main();
		TwoWayJoinWithPredicateAndGroup.main();
		ThreeWayJoinWithPredicateAndReduce.main();
	}
	
	public static class TwoWayCrossJoin {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
					.join(two)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				System.out.println("PARTITOIN");
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class TwoWayJoinWithPredicate {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
					.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				System.out.println("PARTITOIN");
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class TwoWayJoinWithPredicateAndReduce {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = one
					.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
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
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<Entry<String, List<String>>>>> resultFuture = one
					.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.aggregateGroups(s -> s._1(), s -> s._2(), Aggregators::aggregateFlatten)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Entry<String, List<String>>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class ThreeWayJoinWithPredicateAndReduce {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			DStream<String> three = DStream.ofType(String.class, "three");
			
			Future<Stream<Stream<Entry<String, List<String>>>>> resultFuture = one
					.map(line -> line.toUpperCase())
					.join(two.map(line -> line.toUpperCase())).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
					.map(s -> s._1().substring(1).trim() + " - " + s._2().substring(0, s._2().length()-1).trim())
					.join(three)
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
