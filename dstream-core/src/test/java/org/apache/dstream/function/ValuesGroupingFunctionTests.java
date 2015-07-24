package org.apache.dstream.function;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.Aggregators;
import org.apache.dstream.utils.KVUtils;
import org.junit.Assert;
import org.junit.Test;

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
		ValuesGroupingFunction<String, Integer, Entry<String, List<Integer>>> kvsStream = 
				new ValuesGroupingFunction<String, Integer, Entry<String, List<Integer>>>(Aggregators::aggregateFlatten);
		List<Entry<String, List<Integer>>> result = kvsStream.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(4, result.size());
		
		assertArrayEquals(new Integer[]{1,1,1,1,1,1,1}, result.get(0).getValue().toArray());
		assertArrayEquals(new Integer[]{1,1,1,1,1}, result.get(1).getValue().toArray());
		assertArrayEquals(new Integer[]{1,1,1,1,1,1,2}, result.get(2).getValue().toArray());
	}
	
}
