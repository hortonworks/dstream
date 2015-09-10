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
package io.dstream.sql;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import io.dstream.DStream;

/**
 *
 */
public class SQLDStreamTests extends BaseSqlTests {

	@Test
	public void joinSqlAndText() throws Exception {

		DStream<Row> sqlDs = SQLDStream.create("sqlDs");// Convenience factory method. Same as DStream.ofType(Row.class, "sqlDs");
		DStream<String> txtDs = DStream.ofType(String.class, "txtDs");

		Future<Stream<Stream<Entry<String, List<Row>>>>> resultFuture = sqlDs
				.join(txtDs).on(t2 -> t2._1().get(0).equals(Integer.parseInt(t2._2().split("\\s+")[0])))
				.aggregateValues(t2 -> t2._2().split("\\s+")[1], t2 -> t2._1())
				.executeAs("SQLDStreamTests");


		Stream<Stream<Entry<String, List<Row>>>> resultStream = resultFuture.get();
		List<Stream<Entry<String, List<Row>>>> resultStreams = resultStream.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		List<Entry<String, List<Row>>> resultsList = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(4, resultsList.size());
		assertEquals("Canada=[[3, 2000-03-26, Stacy Rodriguez]]", resultsList.get(0).toString());
		assertEquals("China=[[0, 1994-02-23, John Doe], [1, 2013-05-03, Steve Smith]]", resultsList.get(1).toString());
		assertEquals("USA=[[2, 2013-02-13, Steve Rogers]]", resultsList.get(2).toString());
		assertEquals("Ukraine=[[4, 2001-01-30, Camila Wilson]]", resultsList.get(3).toString());

		resultStream.close();

	}

}
