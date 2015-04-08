package org.apache.dstream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.utils.Utils;
import org.junit.Test;

public class StreamProcessingFunctionTests {

	@Test
	public void validateKeyValueFunctionWithComposition(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(Utils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(Utils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(Utils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		KeyValuesStreamAggregator<String, Integer> kvsStream = new KeyValuesStreamAggregator<String, Integer>(Integer::sum);
		Function<Stream<Entry<String, Integer>>, Stream<Entry<String, Integer>>> func = stream -> stream.map(s -> Utils.kv(s.getKey().toUpperCase(), s.getValue()));
		
		KeyValuesAggregatingStreamProcessingFunction<String, Integer, String, Integer> task = new KeyValuesAggregatingStreamProcessingFunction<>(func, kvsStream);
		
		List<Entry<String, Integer>> result = task.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(Utils.kv("BOB", 7), result.get(0));
		Assert.assertEquals(Utils.kv("STACY", 5), result.get(1));
		Assert.assertEquals(Utils.kv("JOHN", 8), result.get(2));
	}
	
	@Test
	public void validateKeyValueFunctionWithoutComposition(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(Utils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(Utils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(Utils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		KeyValuesStreamAggregator<String, Integer> kvsStream = new KeyValuesStreamAggregator<String, Integer>(Integer::sum);
		
		KeyValuesAggregatingStreamProcessingFunction<String, Integer, String, Integer> task = new KeyValuesAggregatingStreamProcessingFunction<>(null, kvsStream);
		
		List<Entry<String, Integer>> result = task.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(Utils.kv("Bob", 7), result.get(0));
		Assert.assertEquals(Utils.kv("Stacy", 5), result.get(1));
		Assert.assertEquals(Utils.kv("John", 8), result.get(2));
	}
}
