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
package dstream.function;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import dstream.function.ValuesAggregatingFunction;
import dstream.support.Aggregators;
import dstream.utils.KVUtils;

public class ValuesGroupingFunctionTests {

	
	@Test
	public void validateValuesGrouping(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(KVUtils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		keyValuesList.add(KVUtils.kv("Oleg", Arrays.asList(new Integer[]{1}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		ValuesAggregatingFunction<String, Integer, Entry<String, List<Integer>>> kvsStream = 
				new ValuesAggregatingFunction<String, Integer, Entry<String, List<Integer>>>(Aggregators::aggregateFlatten);
		List<Entry<String, List<Integer>>> result = kvsStream.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(4, result.size());
		
		assertArrayEquals(new Integer[]{1,1,1,1,1,1,1}, result.get(0).getValue().toArray());
		assertArrayEquals(new Integer[]{1,1,1,1,1}, result.get(1).getValue().toArray());
		assertArrayEquals(new Integer[]{1,1,1,1,1,1,2}, result.get(2).getValue().toArray());
	}
	
}
