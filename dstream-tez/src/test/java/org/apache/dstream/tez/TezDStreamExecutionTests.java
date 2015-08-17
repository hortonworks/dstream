package org.apache.dstream.tez;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;

import dstream.DStream;
import dstream.utils.ExecutionResultUtils;
import dstream.utils.KVUtils;
import dstream.utils.Tuples.Tuple2;
import dstream.utils.Tuples.Tuple3;

public class TezDStreamExecutionTests {
	
	private static String EXECUTION_NAME = TezDStreamExecutionTests.class.getSimpleName();
	
	@After
	public void after(){
		BaseTezTests.clean(EXECUTION_NAME);
	}
	
	@Test
	public void noOperations() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(3, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("The ship drew on and had safely passed the strait, which some volcanic", p1Result.get(0));
//		assertEquals("the forerunner of evil, asked one another what misfortune could have", p1Result.get(1));
		
	}
	
	@Test
	public void classifySource() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.classify(record -> 1)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
	}
	
	@Test
	public void classifyWithPriorTransformation() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toUpperCase())
				.classify(record -> record.substring(0, 9))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);

//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(2, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("HAPPENED ON BOARD.", p1Result.get(0));
//		assertEquals("THE SHIP DREW ON AND HAD SAFELY PASSED THE STRAIT, WHICH SOME VOLCANIC", p1Result.get(3));
	}
	
	@Test
	public void classifyWithPriorKVProducingTransformation() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> KVUtils.kv(s, 1))
				.classify(entry -> entry.getKey().substring(0, 9))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(2, resultPartitionsList.size());
//		
//		// spot check
//		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals(KVUtils.kv("The ship drew on and had safely passed the strait, which some volcanic", 1), p1Result.get(1));
	}
	
	@Test
	public void classifyAfterShuffleOperation() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.classify(entry -> entry.getKey())
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(1, resultPartitionsList.size());
	}
	
	@Test
	public void transformationOnly() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toUpperCase())
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(3, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("THE FORERUNNER OF EVIL, ASKED ONE ANOTHER WHAT MISFORTUNE COULD HAVE", p1Result.get(0));
//		assertEquals("THE SHIP DREW ON AND HAD SAFELY PASSED THE STRAIT, WHICH SOME VOLCANIC", p1Result.get(1));
	}
	
	@Test
	public void computeOnly() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
			.executeAs(EXECUTION_NAME);

		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(3, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
//		assertEquals("POMEGUE, AND APPROACHED THE HARBOR UNDER TOPSAILS, JIB, AND SPANKER, BUT", p2Result.get(0));
//		assertEquals("SO SLOWLY AND SEDATELY BETWEEN THAT THE IDLERS, WITH THAT INSTINCT WHICH IS", p2Result.get(1));
	}
	
	@Test
	public void computeCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("CINACLOV EMOS HCIHW ,TIARTS EHT DESSAP YLEFAS DAH DNA NO WERD PIHS EHT", p1Result.get(0));
	}
	
	@Test
	public void computeShuffleCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s.getKey()).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(3, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("CINACLOV EMOS HCIHW ,TIARTS EHT DESSAP YLEFAS DAH DNA NO WERD PIHS EHT", p1Result.get(0));
	}
	
	@Test
	public void shuffleOnly() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.reduceValues(s -> s, s -> 1, Integer::sum)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(3, resultPartitionsList.size());
//		
//		// spot check
//		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals(KVUtils.kv("The ship drew on and had safely passed the strait, which some volcanic", 1), p1Result.get(0));
	}
	
	@Test
	public void transformationAndShuffle() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.filter(word -> word.length() > 5)
				.map(s -> s.toUpperCase())
				.reduceValues(word -> word, word -> 1, Integer::sum)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
//		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check
//		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals(KVUtils.kv("BETWEEN", 2), p1Result.get(0));
//		assertEquals(KVUtils.kv("INSTINCT", 1), p1Result.get(4));
//		
//		List<Entry<String, Integer>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
//		assertEquals(KVUtils.kv("SAFELY", 1), p3Result.get(1));
//		assertEquals(KVUtils.kv("SPANKER,", 1), p3Result.get(3));
	}
	
	@Test
	public void transformationShuffleTransformationShuffle() throws Exception {
		Future<Stream<Stream<Entry<Integer, List<String>>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.filter(word -> word.length() > 5)
				.map(s -> s.toUpperCase())
				.reduceValues(word -> word, word -> 1, Integer::sum)
				.filter(entry -> entry.getKey().length() > 6)
				.aggregateValues(entry -> entry.getValue(), entry -> entry.getKey())
				.map(s -> {Collections.sort(s.getValue()); return s;})
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<Integer, List<String>>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		List<Stream<Entry<Integer, List<String>>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(2, resultPartitionsList.size());
//		
//		// spot check
//		List<Entry<Integer, List<String>>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("1=[POMEGUE,, INSTINCT, FORERUNNER, HAPPENED, SEDATELY, CALASAREIGNE, ISLANDS;, STRAIT,, MISFORTUNE, SPANKER,, APPROACHED, DOUBLED, TOPSAILS,, IDLERS,, VOLCANIC, ANOTHER]", p1Result.get(0).toString());
//		
//		List<Entry<Integer, List<String>>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
//		assertEquals("2=[BETWEEN]", p2Result.get(0).toString());
	}
	
	@Test
	//TODO validate failure since Tez won't support it (no classification, no predicate)
	public void simpleTwoWayCrossJoin() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();

	}
	
	@Test
	// TODO validate failure since no classification while predicate. Partitions may not match
	public void twoWayJoinWithPredicateNoClassification() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
				.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		List<Stream<Tuple2<String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check	
//		List<Tuple2<String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("[3 Hortonworks, Oleg Zhurakousky 3]", p1Result.get(0).toString());
//		
//		List<Tuple2<String, String>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
//		assertEquals("[2 Amazon, Jeffrey Blackburn 2]", p3Result.get(1).toString());
	}
	
	@Test
	public void twoWayJoinWithPredicateAndClassification() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one").classify(s -> Integer.parseInt(s.split("\\s+")[0]));
		DStream<String> two = DStream.ofType(String.class, "two").classify(s -> Integer.parseInt(s.split("\\s+")[2]));
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two).on(tuple2 -> {
					System.out.println(); 
					return tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1));})
				.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		List<Stream<Tuple2<String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check	
//		List<Tuple2<String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("[3 Hortonworks, Oleg Zhurakousky 3]", p1Result.get(0).toString());
//		
//		List<Tuple2<String, String>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
//		assertEquals("[2 Amazon, Jeffrey Blackburn 2]", p3Result.get(1).toString());
	}
	
	@Test
	public void threeWayJoinWithPredicate() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		DStream<String> three = DStream.ofType(String.class, "three");
		
		Future<Stream<Stream<Tuple3<String, String, String>>>> resultFuture = one
				.join(two)
				.join(three).on(tuple3 -> tuple3._1().substring(0, 1).equals(tuple3._2().substring(tuple3._2().length()-1)) && tuple3._3().startsWith("The"))
				.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple3<String, String, String>>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		List<Stream<Tuple3<String, String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check
//		List<Tuple3<String, String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("[3 Hortonworks, Oleg Zhurakousky 3, The ship drew on and had safely passed the strait, which some volcanic]", p1Result.get(0).toString());
//		
//		List<Tuple3<String, String, String>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
//		assertEquals("[2 Amazon, Jeffrey Blackburn 2, The ship drew on and had safely passed the strait, which some volcanic]", p3Result.get(1).toString());
		
	}
	
	@Test
	public void simpleTwoWayUnion() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("Arun Murthy 3", p1Result.get(0));
//		assertEquals("Jeff Bezos 2", p1Result.get(1).toString());
	}
	
	@Test
	public void simpleThreeWayUnion() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		DStream<String> three = DStream.ofType(String.class, "three");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
				.union(three)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		ExecutionResultUtils.printResults(resultPartitionsStream, true);
//		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
//		assertEquals(4, resultPartitionsList.size());
//		
//		// spot check
//		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
//		assertEquals("Arun Murthy 3", p1Result.get(0));
//		assertEquals("The ship drew on and had safely passed the strait, which some volcanic", p1Result.get(3).toString());
//		
//		List<String> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
//		assertEquals("2 Amazon", p3Result.get(0));
//		assertEquals("Tom McCuch 3", p3Result.get(1).toString());
//		assertEquals("shock has made between the Calasareigne and Jaros islands; had doubled", p3Result.get(3).toString());
	}
}
