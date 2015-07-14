package org.apache.dstream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.support.SerializableFunctionConverters.Predicate;
import org.junit.Test;

public class DistributableStreamToStreamAdapterFunctionTests {
	
	@Test(expected=UnsupportedOperationException.class)
	public void validateUnsupportedOperation(){
		new DistributableStreamToStreamAdapterFunction("foo", null);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void validateNullFunctionFailure(){
		new DistributableStreamToStreamAdapterFunction("map", null);
	}

	@Test
	public void validateMap(){
		Function<String, String> mapFunction = s -> s.toUpperCase();
		DistributableStreamToStreamAdapterFunction f = new DistributableStreamToStreamAdapterFunction("map", mapFunction);
		String result = (String) f.apply(Stream.of("foo")).findFirst().get();
		assertEquals("FOO", result);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void validateflatMap(){
		Function<String, Stream<String>> flatMapFunction = s -> Stream.of(s.split(" "));
		DistributableStreamToStreamAdapterFunction f = new DistributableStreamToStreamAdapterFunction("flatMap", flatMapFunction);	
		Stream<String> resultStream = (Stream<String>) f.apply(Stream.of("foo bar"));
		List<String> result = resultStream.collect(Collectors.toList());
		assertEquals("foo", result.get(0));
		assertEquals("bar", result.get(1));
	}
	
	@Test
	public void validateFilter(){
		Predicate<String> filterFunction = s -> s.equals("foo");
		DistributableStreamToStreamAdapterFunction f = new DistributableStreamToStreamAdapterFunction("filter", filterFunction);
		assertFalse(f.apply(Stream.of("bar")).findFirst().isPresent());
		assertTrue(f.apply(Stream.of("foo")).findFirst().isPresent());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateSequence(){
		Function<String, Stream<String>> flatMapFunction = s -> Stream.of(s.split(" "));
		DistributableStreamToStreamAdapterFunction f0 = new DistributableStreamToStreamAdapterFunction("flatMap", flatMapFunction);	
		
		Predicate<String> filterFunction = s -> s.equals("foo");
		DistributableStreamToStreamAdapterFunction f1 = new DistributableStreamToStreamAdapterFunction("filter", filterFunction);
		
		Function<String, String> mapFunction = s -> s.toUpperCase();
		DistributableStreamToStreamAdapterFunction f2 = new DistributableStreamToStreamAdapterFunction("map", mapFunction);
		
		Function f =  f0.andThen(f1).andThen(f2);
		assertEquals("FOO", ((Stream<String>)f.apply(Stream.of("foo bar"))).findFirst().get());
	}
}
