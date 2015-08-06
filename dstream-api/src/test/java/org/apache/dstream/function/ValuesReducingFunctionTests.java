package org.apache.dstream.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.function.ValuesReducingFunction;
import org.apache.dstream.function.SerializableFunctionConverters.SerBinaryOperator;
import org.apache.dstream.utils.KVUtils;
import org.junit.Assert;
import org.junit.Test;

public class ValuesReducingFunctionTests {

	
	@Test
	public void validateValuesReducing(){
		List<Entry<String, Iterator<Integer>>> keyValuesList = new ArrayList<Entry<String,Iterator<Integer>>>();
		
		keyValuesList.add(KVUtils.kv("Bob", Arrays.asList(new Integer[]{1,1,1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("Stacy", Arrays.asList(new Integer[]{1,1,1,1,1}).iterator()));
		keyValuesList.add(KVUtils.kv("John", Arrays.asList(new Integer[]{1,1,1,1,1,1,2}).iterator()));
		
		Stream<Entry<String,Iterator<Integer>>> sourceStream = keyValuesList.stream();
		// the above stream would be generated from reader provided by the target execution environment (e.g., Tez)
		ValuesReducingFunction<String, Integer, Entry<String, Integer>> kvsStream = 
				new ValuesReducingFunction<String, Integer, Entry<String, Integer>>((SerBinaryOperator<Integer>)Integer::sum);
		List<Entry<String, Integer>> result = kvsStream.apply(sourceStream).collect(Collectors.toList());
		Assert.assertEquals(3, result.size());
		Assert.assertEquals((Integer)7, result.get(0).getValue());
		Assert.assertEquals((Integer)5, result.get(1).getValue());
		Assert.assertEquals((Integer)8, result.get(2).getValue());
	}
	
}
