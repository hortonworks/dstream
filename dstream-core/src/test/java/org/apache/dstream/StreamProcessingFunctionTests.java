package org.apache.dstream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.KVUtils;
import org.junit.Test;

public class StreamProcessingFunctionTests {

	@Test
	public void validateKeyValueFunctionWithComposition(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(KVUtils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		KeyValuesStreamAggregator<String, Integer> kvsStream = new KeyValuesStreamAggregator<String, Integer>(Integer::sum);
		Function<Stream<Entry<String, Integer>>, Stream<Entry<String, Integer>>> func = stream -> stream.map(s -> KVUtils.kv(s.getKey().toUpperCase(), s.getValue()));
		
		KeyValuesAggregatingStreamProcessingFunction<String, Integer, String, Integer> task = new KeyValuesAggregatingStreamProcessingFunction<>(func, kvsStream);
		
		List<Entry<String, Integer>> result = task.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(KVUtils.kv("BOB", 7), result.get(0));
		Assert.assertEquals(KVUtils.kv("STACY", 5), result.get(1));
		Assert.assertEquals(KVUtils.kv("JOHN", 8), result.get(2));
	}
	
	@Test
	public void validateKeyValueFunctionWithoutComposition(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(KVUtils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		KeyValuesStreamAggregator<String, Integer> kvsStream = new KeyValuesStreamAggregator<String, Integer>(Integer::sum);
		
		KeyValuesAggregatingStreamProcessingFunction<String, Integer, String, Integer> task = new KeyValuesAggregatingStreamProcessingFunction<>(null, kvsStream);
		
		List<Entry<String, Integer>> result = task.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(KVUtils.kv("Bob", 7), result.get(0));
		Assert.assertEquals(KVUtils.kv("Stacy", 5), result.get(1));
		Assert.assertEquals(KVUtils.kv("John", 8), result.get(2));
	}
}
