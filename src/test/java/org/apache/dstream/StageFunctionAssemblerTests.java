package org.apache.dstream;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.SerializableHelpers.Predicate;
import org.apache.dstream.utils.ReflectionUtils;
import org.apache.dstream.utils.Utils;
import org.junit.Test;

public class StageFunctionAssemblerTests {

	@Test
	public void test() throws Exception {
		Class<?> clazz = Class.forName("org.apache.dstream.ADSTBuilder$StageFunctionAssembler");
		
		Object stageFunctionAssembler = ReflectionUtils.newInstance(clazz, null, null);
		Method intM = ReflectionUtils.findMethod("addIntrmediate", stageFunctionAssembler.getClass(), void.class, String.class, Object.class);
		intM.setAccessible(true);
		
		intM.invoke(stageFunctionAssembler, "flatMap", flatMap());
//		intM.invoke(stageFunctionAssembler, "filter", filter());
		intM.invoke(stageFunctionAssembler, "map", map());
		
		
//		Function function =  ReflectionUtils.getFieldValue(stageFunctionAssembler, "f", Function.class);
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("oleg nastia oleg oleg seva nastia");
		list.add("oleg dan oleg oleg steve nastia");
		list.add("nastia dan oleg oleg paul nastia");
				
		
		Stream<String> stream = list.stream();
		
//		stream.flatMap(this.flatMap()).map(this.map()).forEach(System.out::println);

		Method buildFuncM = ReflectionUtils.findMethod("buildFunction", stageFunctionAssembler.getClass(), Function.class);
		buildFuncM.setAccessible(true);
		Function f = (Function) buildFuncM.invoke(stageFunctionAssembler);
		f.apply(stream);
		//stream.flatMap(mapper)
		
	}
	
	private Function<String, Stream<String>> flatMap() {
		return (String s) -> Stream.of(s.split("\\s+"));
	}
	
	private Function<String, Entry<String, Integer>> map() {
		return (String s) -> Utils.kv(s, 1);
	}
	
	private Predicate<String> filter() {
		return (String s) -> s.startsWith("o");
	}
}
