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
package io.dstream.support;

import io.dstream.DStream;
import io.dstream.utils.ExecutionResultUtils;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class PartitionIdClassifierTests {

	private static String EXECUTION_NAME = PartitionIdClassifierTests.class.getSimpleName();

	@Test(expected=IllegalStateException.class)
	public void failWithLessThanOnePartitionSize(){
		new PartitionIdClassifier(0);
	}

	@Test
	public void validatePartitionIdClassifier() throws Exception {
		PartitionIdClassifier partitionIdClassifier = new PartitionIdClassifier(4);
		assertEquals(4, partitionIdClassifier.getSize());

		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
															 .flatMap(line -> Stream.of(line.split("\\s+")))
															 .reduceValues(s -> s, s -> 1, Integer::sum).map(s -> {
					if (s.getKey().equals("volcanic")) {
						assertEquals(3, partitionIdClassifier.doGetClassificationId(s.getKey()));
					} else if (s.getKey().equals("safely")) {
						assertEquals(2, partitionIdClassifier.doGetClassificationId(s.getKey()));
					} else if (s.getKey().equals("Calasareigne")) {
						assertEquals(1, partitionIdClassifier.doGetClassificationId(s.getKey()));
					} else if (s.getKey().equals("forerunner")) {
						assertEquals(0, partitionIdClassifier.doGetClassificationId(s.getKey()));
					}
					return s.getKey();
				}).executeAs(EXECUTION_NAME);

		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
	}
}