package org.apache.dstream;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.utils.KVUtils;
import org.junit.Test;

public class KeyValueMappingFunctionTests {

	@Test
	public void validateKVMapper(){
		KeyValueMappingFunction<String, String, Integer> kvFunc = new KeyValueMappingFunction<String, String, Integer>(s -> s, s -> 1);
		List<Entry<String, Integer>> r = kvFunc.apply(Stream.of("hello")).collect(Collectors.toList());
		List<Entry<String, Integer>> result = (List<Entry<String, Integer>>) r;
		Assert.assertEquals(1, result.size());
		Assert.assertEquals(KVUtils.kv("hello", 1), result.get(0));
	}
}
