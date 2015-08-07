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
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.junit.Test;

import dstream.function.KeyValueMappingFunction;
import dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import dstream.support.Aggregators;
import dstream.utils.KVUtils;

public class KeyValueMappingFunctionTests {

	@Test
	public void validateKVMapper(){
		KeyValueMappingFunction<String, String, Integer> kvFunc = new KeyValueMappingFunction<String, String, Integer>(s -> s, s -> 1);
		List<Entry<String, Integer>> result = kvFunc.apply(Stream.of("hello")).collect(Collectors.toList());
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(KVUtils.kv("hello", 1), result.get(0));
	}
	
	@Test
	public void validateKVMapperWithValuesReducer(){
		KeyValueMappingFunction<String, String, Integer> kvFunc = new KeyValueMappingFunction<String, String, Integer>(s -> s, s -> 1, Integer::sum);
		List<Entry<String, Integer>> result = kvFunc.apply(Stream.of("hello", "bye", "hello")).collect(Collectors.toList());
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(KVUtils.kv("hello", 2), result.get(0));
		Assert.assertEquals(KVUtils.kv("bye", 1), result.get(1));
	}
	
	@Test
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void validateKVMapperWithValuesGrouper(){	
		KeyValueMappingFunction<String, String, Object> kvFunc = new KeyValueMappingFunction<String, String, Object>(s -> s, s -> 1, (SerBinaryOperator)Aggregators::aggregateFlatten);
		List<Entry<String, Object>> result = kvFunc.apply(Stream.of("hello", "bye", "hello")).collect(Collectors.toList());
		Assert.assertEquals(2, result.size());
		
		Entry<String, Object> firstResult = result.get(0);
		assertEquals("hello", firstResult.getKey());
		assertArrayEquals(new Integer[]{1,1}, ((List<Integer>)firstResult.getValue()).toArray(new Integer[]{}));
		
		Entry<String, Object> secondResult = result.get(1);
		assertEquals("bye", secondResult.getKey());
		assertEquals(1, secondResult.getValue());
	}
}
