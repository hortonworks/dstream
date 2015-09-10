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

import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.dstream.DStream;
import io.dstream.utils.ExecutionResultUtils;
/**
 * Contains various examples of join operation
 */
public class Union {

	static String EXECUTION_NAME = "Union";

	public static void main(String[] args) throws Exception {
		//		run all
		SimpleTwoWayUnion.main();
	}

	/**
	 * This example demonstrates simple join between two streams.
	 * To ensure correctness of joining data in the distributed environment, classification must
	 * precede any type of streams combine (i.e., join and/or union*). This will ensure
	 * the two+ streams represented as individual partitions have comparable data.
	 *
	 * The following case has two data sets:
	 * <pre>
	 * -one-
	 * 1 Oracle
	 * 2 Amazon
	 * . . .
	 *
	 *  - two-
	 *  Arun Murthy 3
	 *  Larry Ellison 1
	 *  . . .
	 *  </pre>
	 * Classification is performed using the common "id", this ensuring that
	 * <i>'1 Oracle' and 'Larry Ellison 1'</i> will end up in the same partition.
	 *
	 * In this example you can also see a nice side-effect of 'classification',
	 * since this example uses 'dstream.parallelism=3' configuration.
	 * Since variation of classification values matches the 'parallelism' value (3)
	 * the result resembles 'join' behavior since each of the three partitions
	 * only contain data relevant to classification id, giving you the following result:
	 * <pre>
	 * =&gt; PARTITION:0
	 * 3 Hortonworks
	 * Rob Bearden 3
	 * Herb Cunitz 3
	 * Tom McCuch 3
	 * Oleg Zhurakousky 3
	 * Arun Murthy 3
	 *
	 * =&gt; PARTITION:1
	 * 1 Oracle
	 * Larry Ellison 1
	 * Thomas Kurian 1
	 *
	 * =&gt; PARTITION:2
	 * 2 Amazon
	 * Jeff Bezos 2
	 * Jeffrey Blackburn 2
	 * </pre>
	 */
	public static class SimpleTwoWayUnion {
		public static void main(String... args) throws Exception {
			DStream<String> one = DStream.ofType(String.class, "one").classify(s -> s.split("\\s+")[0]);
			DStream<String> two = DStream.ofType(String.class, "two").classify(s -> s.split("\\s+")[2]);

			Future<Stream<Stream<String>>> resultFuture = one
					.union(two)
					.executeAs(EXECUTION_NAME);

			Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
			ExecutionResultUtils.printResults(resultPartitionsStream, true);
		}
	}
}
