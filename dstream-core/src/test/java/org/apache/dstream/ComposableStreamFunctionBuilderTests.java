package org.apache.dstream;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.KVUtils;
import org.apache.dstream.utils.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;

public class ComposableStreamFunctionBuilderTests {

	@SuppressWarnings({ "unchecked"})
	@Test
	public void validateComposition() throws Exception {
		ComposableStreamFunction composableStreamFunction = new ComposableStreamFunction();

		composableStreamFunction.add(new DistributableStreamToStreamAdapterFunction("flatMap", flatMap()));
		composableStreamFunction.add(new DistributableStreamToStreamAdapterFunction("filter", filter()));
		composableStreamFunction.add(new DistributableStreamToStreamAdapterFunction("map", map()));
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("foo bar baz bar baz");
		list.add("foo bar bar bazz zad bar zad foo");
		list.add("dan foo bar");
					
		Stream<String> stream = list.stream();
		
		List<Entry<String, Integer>> result = (List<Entry<String, Integer>>) composableStreamFunction.apply(stream).collect(Collectors.toList());
		Assert.assertEquals(4, result.size());
		for (Entry<String, Integer> entry : result) {
			Assert.assertEquals("foo", entry.getKey());
			Assert.assertEquals((Integer)1, entry.getValue());
		}
	}
	
	private Function<String, Stream<String>> flatMap() {
		return (String s) -> Stream.of(s.split("\\s+"));
	}
	
	private Function<String, Entry<String, Integer>> map() {
		return (String s) -> KVUtils.kv(s, 1);
	}
	
	private Predicate<String> filter() {
		return (String s) -> s.startsWith("foo");
	}
}
