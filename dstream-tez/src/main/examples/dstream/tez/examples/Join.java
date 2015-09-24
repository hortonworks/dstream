package dstream.tez.examples;

import static io.dstream.utils.Tuples.Tuple2.tuple2;
import static io.dstream.utils.Tuples.Tuple4.tuple4;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.dstream.DStream;
import io.dstream.utils.Tuples.Tuple2;
import io.dstream.utils.Tuples.Tuple4;

public class Join {

	static String EXECUTION_NAME = "Join";

	public static void main(String[] args) throws Exception {
		//run all
		TwoWayJoin.main();
		FourWayJoin.main();
		SampleUtils.clean(EXECUTION_NAME);
	}

	/**
	 * This example demonstrates simple join between two streams.
	 * To ensure correctness of joining data in the distributed environment, classification must
	 * precede any type of streams combine (i.e., join and/or union*). This will ensure
	 * the two+ streams represented as individual partitions have comparable data.
	 *
	 * The following case has two data sets:
	 * 	-one-
	 * 1 Oracle
	 * 2 Amazon
	 * . . .
	 *
	 *  - two-
	 *  Arun Murthy 3
	 *  Larry Ellison 1
	 *  . . .
	 *
	 * Classification is performed using the common "id", this ensuring that
	 * '1 Oracle' and 'Larry Ellison 1' will end up in the same partition.
	 */
	public static class TwoWayJoin{
		public static void main(String... args) throws Exception {
			SampleUtils.clean(EXECUTION_NAME);

			DStream<String> hash = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);;
			DStream<String> probe = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);;

			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash
					.join(probe).on(t2 -> t2._1().substring(0, 1).equals(t2._2().substring(t2._2().length()-1)))
					.executeAs(EXECUTION_NAME);

			Stream<Stream<Tuple2<String, String>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();// will close Tez client

			SampleUtils.clean(EXECUTION_NAME);
		}
	}

	/**
	 * This example shows a sample of joining more then two data sets with some transformation
	 * as well as multiple predicates
	 */
	public static class FourWayJoin {
		public static void main(String... args) throws Exception {
			SampleUtils.clean(EXECUTION_NAME);

			DStream<String> one = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);
			DStream<String> two = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);
			DStream<String> three = DStream.ofType(String.class, "three").classify(a -> a.split("\\s+")[0]);
			DStream<String> four = DStream.ofType(String.class, "four").classify(a -> a.split("\\s+")[0]);

			Future<Stream<Stream<Tuple4<String, String, String, String>>>> resultFuture = one
					.join(two)
					.filter(t2 -> t2._1().contains("Hortonworks"))
					.map(t2 -> tuple2(t2._1().toUpperCase(), t2._2().toUpperCase()))
					.join(three)
					.join(four).on(t3 -> {
						String v1 = t3._1()._1().split("\\s+")[0];
						String v2 = t3._1()._2().split("\\s+")[2];
						String v3 = t3._2().split("\\s+")[0];
						String v4 = t3._3().split("\\s+")[0];
						return v1.equals(v2) && v1.equals(v3) && v1.equals(v4);
					})
					.map(t3 -> tuple4(t3._1()._1(), t3._1()._2(), t3._2(), t3._3()))
					.executeAs(EXECUTION_NAME);

			Stream<Stream<Tuple4<String, String, String, String>>> result = resultFuture.get();
			result.forEach(resultPartitionStream -> {
				resultPartitionStream.forEach(System.out::println);
			});
			result.close();// will close Tez client
			SampleUtils.clean(EXECUTION_NAME);
		}
	}
}
