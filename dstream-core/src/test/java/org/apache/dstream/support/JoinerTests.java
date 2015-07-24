package org.apache.dstream.support;

import static org.apache.dstream.utils.Tuples.Tuple2.tuple2;
import static org.apache.dstream.utils.Tuples.Tuple3.tuple3;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.apache.dstream.utils.Tuples.Tuple3;
import org.junit.Test;

public class JoinerTests {
	
	@SuppressWarnings("unchecked")
	@Test
	public void crossJoinTwoStreams(){
		Stream<String> one = Stream.of("Eeny", "meeny");
		Stream<String> two = Stream.of("miny", "moe");
		
		Joiner joiner = new Joiner();
		
		Stream<Tuple2<String, String>> resultStream = (Stream<Tuple2<String, String>>) joiner.join(one, two);
		List<Tuple2<String, String>> resultList = resultStream.collect(Collectors.toList());
		assertEquals(4, resultList.size());
		assertEquals(tuple2("Eeny", "miny"), resultList.get(0));
		assertEquals(tuple2("meeny", "miny"), resultList.get(1));
		assertEquals(tuple2("Eeny", "moe"), resultList.get(2));
		assertEquals(tuple2("meeny", "moe"), resultList.get(3));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void crossJoinThreeStreams(){
		Stream<String> one = Stream.of("Eeny", "meeny");
		Stream<String> two = Stream.of("miny", "moe");
		Stream<String> three = Stream.of("Catch", "a", "tiger");
		
		Joiner joiner = new Joiner();
		
		Stream<Tuple3<String, String, String>> resultStream = (Stream<Tuple3<String, String, String>>) joiner.join(one, two, three);
		
		List<Tuple3<String, String, String>> resultList = resultStream.collect(Collectors.toList());
		assertEquals(12, resultList.size());
		assertEquals(tuple3("Eeny", "miny", "Catch"), resultList.get(0));
		assertEquals(tuple3("meeny", "miny", "Catch"), resultList.get(1));
		assertEquals(tuple3("Eeny", "moe", "Catch"), resultList.get(2));
		assertEquals(tuple3("meeny", "moe", "Catch"), resultList.get(3));
		assertEquals(tuple3("Eeny", "miny", "a"), resultList.get(4));
		assertEquals(tuple3("meeny", "miny", "a"), resultList.get(5));
		assertEquals(tuple3("Eeny", "moe", "a"), resultList.get(6));
		assertEquals(tuple3("meeny", "moe", "a"), resultList.get(7));
		assertEquals(tuple3("Eeny", "miny", "tiger"), resultList.get(8));
		assertEquals(tuple3("meeny", "miny", "tiger"), resultList.get(9));
		assertEquals(tuple3("Eeny", "moe", "tiger"), resultList.get(10));
		assertEquals(tuple3("meeny", "moe", "tiger"), resultList.get(11));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void twoWayWithPredicate(){
		Stream<String> one = Stream.of("Eeny", "meeny");
		Stream<String> two = Stream.of("miny", "moe");
		
		Joiner joiner = new Joiner(s -> ((Tuple2<String, String>)s)._1.contains("m") && ((Tuple2<String, String>)s)._2.contains("m"));
		
		Stream<Tuple2<String, String>> resultStream = (Stream<Tuple2<String, String>>) joiner.join(one, two);
		List<Tuple2<String, String>> resultList = resultStream.collect(Collectors.toList());
		assertEquals(2, resultList.size());
		assertEquals(tuple2("meeny", "miny"), resultList.get(0));
		assertEquals(tuple2("meeny", "moe"), resultList.get(1));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void crossMixedWithPredicate(){
		Stream<String> one = Stream.of("Eeny", "meeny");
		Stream<String> two = Stream.of("miny", "moe");
		Stream<String> three = Stream.of("Catch", "a", "tiger");
		
		Joiner joiner = new Joiner(s -> ((Tuple2<String, String>)s)._1.contains("m") && ((Tuple2<String, String>)s)._2.contains("m"));
		Stream<Tuple3<String, String, String>> resultStream = (Stream<Tuple3<String, String, String>>) joiner.join(one, two, three);
		
		List<Tuple3<String, String, String>> resultList = resultStream.collect(Collectors.toList());
		assertEquals(6, resultList.size());
		assertEquals(tuple3("meeny", "miny", "Catch"), resultList.get(0));
		assertEquals(tuple3("meeny", "moe", "Catch"), resultList.get(1));
		assertEquals(tuple3("meeny", "miny", "a"), resultList.get(2));
		assertEquals(tuple3("meeny", "moe", "a"), resultList.get(3));
		assertEquals(tuple3("meeny", "miny", "tiger"), resultList.get(4));
		assertEquals(tuple3("meeny", "moe", "tiger"), resultList.get(5));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void threeWayWithPredicates(){
		Stream<String> one = Stream.of("Eeny", "meeny");
		Stream<String> two = Stream.of("miny", "moe");
		Stream<String> three = Stream.of("Catch", "a", "tiger");
		
		Predicate p1 = s -> ((Tuple2<String, String>)s)._1.contains("m") && ((Tuple2<String, String>)s)._2.contains("m");
		Predicate p2 = s -> ((Tuple3<String, String, String>)s)._3.contains("r");
		
		Joiner joiner = new Joiner(p1, p2);
		
		Stream<Tuple3<String, String, String>> resultStream = (Stream<Tuple3<String, String, String>>) joiner.join(Stream.of(one, two, three));
		List<Tuple3<String, String, String>> resultList = resultStream.collect(Collectors.toList());
		assertEquals(2, resultList.size());
		assertEquals(tuple3("meeny", "miny", "tiger"), resultList.get(0));
		assertEquals(tuple3("meeny", "moe", "tiger"), resultList.get(1));
	}
}
