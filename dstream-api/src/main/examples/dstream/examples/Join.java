/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.examples;

import static io.dstream.utils.Tuples.Tuple2.tuple2;
import static io.dstream.utils.Tuples.Tuple4.tuple4;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.dstream.DStream;
import io.dstream.utils.ExecutionResultUtils;
import io.dstream.utils.Tuples.Tuple2;
import io.dstream.utils.Tuples.Tuple4;
/**
 * Contains various examples of join operation
 */
public class Join {

	static String EXECUTION_NAME = "Join";

	public static void main(String[] args) throws Exception {
		//		run all
		TwoWayJoin.main();
		FourWayJoin.main();
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
	public static class TwoWayJoin {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one").classify(s -> s.split("\\s+")[0]);
			DStream<String> two = DStream.ofType(String.class, "two").classify(s -> s.split("\\s+")[2]);

			Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
					.join(two).on(t2 -> t2._1().split("\\s+")[0].equals(t2._2().split("\\s+")[2]))
					.executeAs(EXECUTION_NAME);

			Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}

	/**
	 * This example shows a sample of joining more then two data sets with some transformation
	 * as well as multiple predicates
	 */
	public static class FourWayJoin {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);
			DStream<String> two = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);
			DStream<String> three = DStream.ofType(String.class, "three").classify(a -> a.split("\\s+")[0]);
			DStream<String> four = DStream.ofType(String.class, "four").classify(a -> a.split("\\s+")[0]);

			Future<Stream<Stream<Entry<String, List<Tuple4<String, String, String, String>>>>>> resultFuture = one
					.join(two)//.on(t2 -> t2._1().contains("Hortonworks"))
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
					.aggregateValues(t4 -> t4._1(), t4 -> t4)
					.executeAs(EXECUTION_NAME);

			Stream<Stream<Entry<String, List<Tuple4<String, String, String, String>>>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
}
