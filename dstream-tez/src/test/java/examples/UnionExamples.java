package examples;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.tez.BaseTezTests;

import dstream.DStream;
import dstream.utils.KVUtils;

public class UnionExamples {
	
	static String EXECUTION_NAME = "UnionExamples";

	public static void main(String[] args) throws Exception {
		// run all
		SimpleUnion.main();
		ThreeWayUnionWithTransformationInBetween.main();
		ThreeWayUnionWithShuffleInBetween.main();
		BaseTezTests.clean(EXECUTION_NAME);
	}
	
	public static class SimpleUnion {
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			
			Future<Stream<Stream<String>>> resultFuture = one
					.union(two)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<String>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class ThreeWayUnionWithTransformationInBetween{
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			DStream<String> three = DStream.ofType(String.class, "three");
			
			Future<Stream<Stream<String>>> resultFuture = one
					.union(two)
					.map(line -> line.toUpperCase())
					.union(three)
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<String>> result = resultFuture.get();
			
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}
	
	public static class ThreeWayUnionWithShuffleInBetween{
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> one = DStream.ofType(String.class, "one");
			DStream<String> two = DStream.ofType(String.class, "two");
			DStream<String> three = DStream.ofType(String.class, "three");
			
			Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = one
					.union(two)
					.map(line -> line.toUpperCase())
					.reduceValues(s -> s, s -> 1, Integer::sum)
					.union(three.map(s -> KVUtils.kv(s.toUpperCase(), 1)))
					.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Entry<String, Integer>>> result = resultFuture.get();
			
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();
		}
	}

}
