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
import static dstream.utils.Tuples.Tuple2.tuple2;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import dstream.DStream;
import dstream.utils.ExecutionResultUtils;
import dstream.utils.Tuples.Tuple2;

/**
 * Contains various examples of basic transformation and shuffle operations
 *
 */
public class BasicTransformationAndShuffles {
	
	static String EXECUTION_NAME = "BasicTransformationAndShuffles";

	public static void main(String[] args) throws Exception {
		//run all
		NoTransformationNoShuffles.main();
		TransformationNoShuffles.main();
		TransformationAndReduceShuffle.main();
		TransformationAndAggregateShuffle.main(args);
		CombinationOfTransformationAndShuffles.main();
	}
	
	public static class NoTransformationNoShuffles {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.executeAs(EXECUTION_NAME);

			Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
	
	public static class TransformationNoShuffles {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.map(s -> s.toUpperCase())
				.executeAs(EXECUTION_NAME);

			Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
	
	public static class TransformationAndReduceShuffle {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.reduceValues(word -> word, word -> 1, Integer::sum)
				.executeAs(EXECUTION_NAME);
			
			// each stream within a stream represents a partition essentially giving you access to each result partition
			Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
	
	public static class TransformationAndAggregateShuffle {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<Entry<Integer, List<String>>>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.aggregateValues(word -> word.length(), word -> word)
				.executeAs(EXECUTION_NAME);
			
			Stream<Stream<Entry<Integer, List<String>>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
	
	public static class CombinationOfTransformationAndShuffles  {
		public static void main(String... args) throws Exception {
			Future<Stream<Stream<Entry<Integer, List<Tuple2<String, Integer>>>>>> resultFuture = DStream.ofType(String.class, "wc")
					.flatMap(line -> Stream.of(line.split("\\s+")))
					.filter(word -> word.length() > 4)
					.reduceValues(word -> word, word -> 1, Integer::sum)
					.map(entry -> tuple2(entry.getKey(), entry.getValue()))
					.aggregateValues(s -> s._1().length(), s -> s)
				.executeAs(EXECUTION_NAME);
			
			// each stream within a stream represents a partition essentially giving you access to each result partition
			Stream<Stream<Entry<Integer, List<Tuple2<String, Integer>>>>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
}
