package org.apache.dstream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.utils.KVUtils;
import org.junit.Assert;
import org.junit.Test;

public class KeyValuesStreamAggregatorTests {

	/**
	 * This tests resembles certain shuffle behaviors where shuffle groups values essentially 
	 * resulting in the following Key/Value semantics <K,V[]>
	 * So KeyValuesAggregatableStream allows user to apply similar lazy semantics as they would on the 
	 * Stream while adding additional operation 'aggregate'.
	 */
	@Test
	public void validateKeyValuesAggregation(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(KVUtils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		KeyValuesStreamAggregatingFunction<String, Integer> kvsStream = new KeyValuesStreamAggregatingFunction<String, Integer>(Integer::sum);
		List<Entry<String, Integer>> result = kvsStream.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(3, result.size());
		Assert.assertEquals((Integer)7, result.get(0).getValue());
		Assert.assertEquals((Integer)5, result.get(1).getValue());
		Assert.assertEquals((Integer)8, result.get(2).getValue());
	}
	
}
