package dstream.tez.examples;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.tez.BaseTezTests;

import dstream.DStream;

public class Union {
	
	static String EXECUTION_NAME = "Union";

	public static void main(String[] args) throws Exception {
		// run all
		SimpleUnion.main();
		BaseTezTests.clean(EXECUTION_NAME);
	}
	
	public static class SimpleUnion {
		public static void main(String... args) throws Exception {
			BaseTezTests.clean(EXECUTION_NAME);
			
			DStream<String> one = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);
			DStream<String> two = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);
			
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
}
