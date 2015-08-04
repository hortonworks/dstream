package examples;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DStream;
import org.apache.dstream.tez.BaseTezTests;
import org.apache.dstream.utils.Tuples.Tuple2;

public class Join {
	public static void main(String[] args) throws Exception {
//		simpleTwoWayCrossJoin();
//		simpleTwoWayJoin();
		simpleTwoWayJoinWithReduce();
		BaseTezTests.clean("WordCount");
	}
	
	private static void simpleTwoWayCrossJoin() throws Exception {
		DStream<String> hash = DStream.ofType(String.class, "hash");
		DStream<String> probe = DStream.ofType(String.class, "probe");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash
				.join(probe)
				.executeAs("Join");
		
		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
		result.forEach(resultPartitionStream -> {
			resultPartitionStream.forEach(System.out::println);
		});
	}
	
	private static void simpleTwoWayJoin() throws Exception {
		DStream<String> hash = DStream.ofType(String.class, "hash");
		DStream<String> probe = DStream.ofType(String.class, "probe");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash
				.join(probe).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
				.executeAs("Join");
		
		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
		result.forEach(resultPartitionStream -> {
			resultPartitionStream.forEach(System.out::println);
		});
	}
	
	private static void simpleTwoWayJoinWithReduce() throws Exception {
		DStream<String> hash = DStream.ofType(String.class, "hash");
		DStream<String> probe = DStream.ofType(String.class, "probe");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = hash
				.join(probe).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
				.reduceGroups(s -> s._1(), s -> 1, Integer::sum)
				.map(s -> {
					System.out.println();
					return s;})
				.executeAs("Join");
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get();
		List<Stream<Entry<String, Integer>>> lResult = result.collect(Collectors.toList());
		List r = lResult.get(0).collect(Collectors.toList());
		System.out.println(r);
//		result.forEach(resultPartitionStream -> {
//			resultPartitionStream.forEach(System.out::println);
//		});
	}
}
