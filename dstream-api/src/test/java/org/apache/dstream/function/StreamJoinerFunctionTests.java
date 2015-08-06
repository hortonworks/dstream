package org.apache.dstream.function;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.instrument.TupleEnhancer;
import org.apache.dstream.support.Tuple7;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.apache.dstream.utils.Tuples.Tuple4;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamJoinerFunctionTests {
	
	List<String> la = Arrays.asList(new String[]{"A-foo", "A-bar", "A-baz"});
	List<String> lb = Arrays.asList(new String[]{"B-foo", "B-bar"});
	List<String> lc = Arrays.asList(new String[]{"C-foo", "C-bar", "C-baz", "C-abc"});
	List<String> ld = Arrays.asList(new String[]{"D-foo", "D-bar", "D-baz", "D-abc"});
	
	@BeforeClass
	public static void beforeClass(){
		TupleEnhancer.enhance();
	}
	
	@Test(expected=IllegalStateException.class)
	public void failWithLessThenTwoStreams(){
		Stream<Stream<?>> streams = Stream.of(la.stream());
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		joiner.apply(streams);
	}
	
	@Test
	public void twoWayCrossJoin(){
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream());
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		Stream<?> mergedStream = joiner.apply(streams);
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(la.size()*lb.size(), result.size());
		assertEquals("[A-foo, B-foo]", result.get(0));
		assertEquals("[A-foo, B-bar]", result.get(1));
		assertEquals("[A-bar, B-foo]", result.get(2));
		assertEquals("[A-bar, B-bar]", result.get(3));
		assertEquals("[A-baz, B-foo]", result.get(4));
		assertEquals("[A-baz, B-bar]", result.get(5));
	}
	
	@Test
	public void twoWayCrossJoinWithTransformatinPointNoFunction(){
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream());
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		joiner.addCheckPoint(1);
		Stream<?> mergedStream = joiner.apply(streams);
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(la.size()*lb.size(), result.size());
		assertEquals("[A-foo, B-foo]", result.get(0));
		assertEquals("[A-foo, B-bar]", result.get(1));
		assertEquals("[A-bar, B-foo]", result.get(2));
		assertEquals("[A-bar, B-bar]", result.get(3));
		assertEquals("[A-baz, B-foo]", result.get(4));
		assertEquals("[A-baz, B-bar]", result.get(5));
	}
	
	@Test
	public void twoWayPredicateJoin(){
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream());
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		
		Predicate<Tuple2<String, String>> p = tuple2 -> tuple2._1().endsWith(tuple2._2().substring(1));
		joiner.addCheckPoint(1);
		joiner.addTransformationOrPredicate("filter", p);
		Stream<?> mergedStream = joiner.apply(streams);
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(2, result.size());
		assertEquals("[A-foo, B-foo]", result.get(0));
		assertEquals("[A-bar, B-bar]", result.get(1));
	}
	
	@Test
	public void starCrossJoin(){ // 4 way
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream(), lc.stream(), ld.stream());
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		
		Stream<?> mergedStream = joiner.apply(streams);
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(la.size()*lb.size()*lc.size()*ld.size(), result.size());
		//spot check
		assertEquals("[A-foo, B-foo, C-foo, D-foo]", result.get(0));
		assertEquals("[A-foo, B-bar, C-foo, D-foo]", result.get(16));
		assertEquals("[A-foo, B-bar, C-baz, D-abc]", result.get(27));
		assertEquals("[A-bar, B-foo, C-baz, D-foo]", result.get(40));
		assertEquals("[A-bar, B-foo, C-abc, D-abc]", result.get(47));
		assertEquals("[A-baz, B-foo, C-bar, D-baz]", result.get(70));
	}
	
	@Test
	public void starSinglePredicateAtEndJoin(){ // 4 way
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream(), lc.stream(), ld.stream());
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		
		Predicate<Tuple4<String, String, String, String>> p = tuple4 -> 
			tuple4._1().endsWith(tuple4._2().substring(1)) &&
			tuple4._2().endsWith(tuple4._3().substring(1)) &&
			tuple4._3().endsWith(tuple4._4().substring(1));

		joiner.addCheckPoint(3);
		joiner.addTransformationOrPredicate("filter", p);
		
		Stream<?> mergedStream = joiner.apply(streams);
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(2, result.size());
		assertEquals("[A-foo, B-foo, C-foo, D-foo]", result.get(0));
		assertEquals("[A-bar, B-bar, C-bar, D-bar]", result.get(1));
	}
	
	@Test
	public void starMultiPredicatesAndTransformations(){ 
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream(), lc.stream(), ld.stream(), Stream.of("hello"));
		
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		
		// First 2
		Predicate<Tuple2<String, String>> p = tuple2 -> tuple2._1().endsWith(tuple2._2().substring(1));	
		joiner.addCheckPoint(1);
		joiner.addTransformationOrPredicate("filter", p);
		
		Function<?,?> f1 = tuple2 -> tuple2.toString().toUpperCase();
		joiner.addTransformationOrPredicate("map", f1);
		// =======
		
		// 3
		Predicate<Tuple2<String, String>> p2 = tuple2 -> tuple2._2().equals("C-baz");
		joiner.addCheckPoint(1);
		joiner.addTransformationOrPredicate("filter", p2);
		
		// 4
		Predicate<Tuple4<String, String, String, String>> p3 = tuple4 -> tuple4._3().endsWith(tuple4._2().substring(1));
		joiner.addCheckPoint(2);
		joiner.addTransformationOrPredicate("filter", p3);
		
		Function<?,?> m2 = tuple3 -> tuple3.toString();
		joiner.addTransformationOrPredicate("map", m2);
	
		Stream<?> mergedStream = joiner.apply(streams);
		
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(2, result.size());
		assertEquals("[[A-FOO, B-FOO], C-baz, D-baz, hello]", result.get(0));
		assertEquals("[[A-BAR, B-BAR], C-baz, D-baz, hello]", result.get(1));
	}
	
	@Test
	public void starWithTransformationsSomePredicates(){ 
		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream(), lc.stream(), ld.stream(), Stream.of("hello"));
		
		StreamJoinerFunction joiner = new StreamJoinerFunction();
		
		// First 2
		joiner.addCheckPoint(1);
		
		Function<?,?> f1 = tuple2 -> tuple2.toString().toUpperCase();
		joiner.addTransformationOrPredicate("map", f1);
		// =======
		
		// 3
		Predicate<Tuple2<String, String>> p2 = tuple2 -> tuple2._2().equals("C-baz");
		joiner.addCheckPoint(1);
		joiner.addTransformationOrPredicate("filter", p2);
		
		// 4
		joiner.addCheckPoint(2);
		
		Function<?,?> m2 = tuple3 -> tuple3.toString();
		joiner.addTransformationOrPredicate("map", m2);
	
		Stream<?> mergedStream = joiner.apply(streams);
		
		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
		assertEquals(24, result.size());
		// spot check
		assertEquals("[[A-FOO, B-FOO], C-baz, D-foo, hello]", result.get(0));
		assertEquals("[[A-BAR, B-FOO], C-baz, D-baz, hello]", result.get(10));
		assertEquals("[[A-BAZ, B-FOO], C-baz, D-bar, hello]", result.get(17));
	}
	
//	@Test
//	public void withDynamicTuple(){ // 7 way but only have 6
//		Stream<String> le = Stream.of("E-foo", "E-bar");
//		Stream<String> lf = Stream.of("F-foo");
//		Stream<String> lg = Stream.of("G-foo", "G-bar");
//		Stream<Stream<?>> streams = Stream.of(la.stream(), lb.stream(), lc.stream(), ld.stream(), le, lf, lg);
//		
//		StreamJoinerFunction joiner = new StreamJoinerFunction();
//		
//		Predicate<Tuple7<String, String, String, String, String, String, String>> p = tuple7 -> 
//			tuple7._1().endsWith(tuple7._2().substring(1)) &&
//			tuple7._2().endsWith(tuple7._3().substring(1)) &&
//			tuple7._3().endsWith(tuple7._4().substring(1)) &&
//			tuple7._4().endsWith(tuple7._5().substring(1)) &&
//			tuple7._5().endsWith(tuple7._6().substring(1)) &&
//			tuple7._6().endsWith(tuple7._7().substring(1));
//			
//		joiner.addCheckPoint(6);
//		joiner.addTransformationOrPredicate("filter", p);
//		
//		Stream<?> mergedStream = joiner.apply(streams);
//		List<String> result = mergedStream.map(s -> s.toString()).peek(System.out::println).collect(Collectors.toList());
//		assertEquals(1, result.size());
//		assertEquals("[A-foo, B-foo, C-foo, D-foo, E-foo, F-foo, G-foo]", result.get(0));
//	}
}
