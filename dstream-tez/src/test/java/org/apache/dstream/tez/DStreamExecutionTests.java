package org.apache.dstream.tez;

import static dstream.utils.Tuples.Tuple2.tuple2;
import static dstream.utils.Tuples.Tuple4.tuple4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dstream.DStream;
import dstream.utils.ExecutionResultUtils;
import dstream.utils.KVUtils;
import dstream.utils.StringUtils;
import dstream.utils.Tuples.Tuple2;
import dstream.utils.Tuples.Tuple4;

public class DStreamExecutionTests extends BaseTezTests {
	
	private static String EXECUTION_NAME = DStreamExecutionTests.class.getSimpleName();
	
	@Before
	public void before(){
		clean(EXECUTION_NAME);
	}
	@After
	public void after(){
		clean(EXECUTION_NAME);
	}
	
	@Test
	public void noOperations() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("The ship drew on and had safely passed the strait, which some volcanic", p1Result.get(0));
		assertEquals("shock has made between the Calasareigne and Jaros islands; had doubled", p1Result.get(1));
	}
	
	@Test
	public void classifySource() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.classify(record -> 1)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("shock has made between the Calasareigne and Jaros islands; had doubled", p2Result.get(1));
	}
	
	@Test
	public void classifyWithPriorTransformation() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toLowerCase())
				.classify(record -> record.substring(0, 1))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("happened on board.", p1Result.get(0));
		assertEquals("the ship drew on and had safely passed the strait, which some volcanic", p1Result.get(2));
	}
	
	@Test
	public void classifyWithPriorKVProducingTransformation() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> KVUtils.kv(s, 1))
				.classify(entry -> entry.getKey().substring(0, 6))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(KVUtils.kv("The ship drew on and had safely passed the strait, which some volcanic", 1), p1Result.get(0));
		
		List<Entry<String, Integer>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(KVUtils.kv("Pomegue, and approached the harbor under topsails, jib, and spanker, but", 1), p2Result.get(0));
	}
	
	@Test
	public void classifyWithTransformationAfterClassification() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> KVUtils.kv(s, 1))
				.classify(entry -> entry.getKey().substring(0, 5))
				.map(entry -> KVUtils.kv(new StringBuilder(entry.getKey()).reverse().toString(), entry.getValue()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Entry<String, Integer>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(KVUtils.kv("evah dluoc enutrofsim tahw rehtona eno deksa ,live fo rennurerof eht", 1), p2Result.get(1));
	}
	
	@Test
	public void classifyAfterShuffleOperation() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.classify(entry -> 1)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		List<Entry<String, Integer>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(KVUtils.kv("could", 1), p2Result.get(7));
	}
	
	@Test
	public void classifyAfterShuffleAndShuffleAgain() throws Exception {
		Future<Stream<Stream<Entry<String, List<Entry<String, Integer>>>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.classify(entry -> 1)
				.aggregateValues(e -> e.getKey().substring(0, 1), e -> e)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, List<Entry<String, Integer>>>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, List<Entry<String, Integer>>>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Entry<String, List<Entry<String, Integer>>>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("b=[between=2, but=1, board.=1]", p1Result.get(3).toString());
		assertEquals("j=[jib,=1]", p1Result.get(7).toString());
		
		List<Entry<String, List<Entry<String, Integer>>>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("s=[strait,=1, sedately=1, safely=1, ship=1, shock=1, slowly=1, so=1, some=1, spanker,=1]", p2Result.get(7).toString());
	}
	
	@Test
	public void subsequentClassify() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.classify(s -> s.getKey().substring(0, 1))
				.classify(s -> s.getKey().substring(0, 2))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(KVUtils.kv("Calasareigne", 1), p1Result.get(0));
		assertEquals(KVUtils.kv("asked", 1), p1Result.get(2));
		assertEquals(KVUtils.kv("that", 2), p1Result.get(13));
	}
	
	@Test
	public void transformationOnly() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toUpperCase())
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("THE SHIP DREW ON AND HAD SAFELY PASSED THE STRAIT, WHICH SOME VOLCANIC", p1Result.get(0));
		assertEquals("THE FORERUNNER OF EVIL, ASKED ONE ANOTHER WHAT MISFORTUNE COULD HAVE", p1Result.get(2));
	}
	
	@Test
	public void computeOnly() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
			.executeAs(EXECUTION_NAME);

		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("POMEGUE, AND APPROACHED THE HARBOR UNDER TOPSAILS, JIB, AND SPANKER, BUT", p2Result.get(0));
		assertEquals("SO SLOWLY AND SEDATELY BETWEEN THAT THE IDLERS, WITH THAT INSTINCT WHICH IS", p2Result.get(1));
	}
	
	@Test
	public void computeCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("CINACLOV EMOS HCIHW ,TIARTS EHT DESSAP YLEFAS DAH DNA NO WERD PIHS EHT", p1Result.get(0));
	}
	
	@Test
	public void computeClassifyCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.classify(s -> s.split(" ")[0].length())
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(".DRAOB NO DENEPPAH", p1Result.get(2));
	}
	
	@Test
	public void computeShuffleCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s.getKey()).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("CINACLOV EMOS HCIHW ,TIARTS EHT DESSAP YLEFAS DAH DNA NO WERD PIHS EHT", p1Result.get(3));
	}
	
	@Test
	public void shuffleOnly() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.reduceValues(s -> s, s -> 1, Integer::sum)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(KVUtils.kv("The ship drew on and had safely passed the strait, which some volcanic", 1), p1Result.get(0));
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
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(KVUtils.kv("BETWEEN", 2), p1Result.get(0));
		assertEquals(KVUtils.kv("INSTINCT", 1), p1Result.get(4));
		
		List<Entry<String, Integer>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(KVUtils.kv("ANOTHER", 1), p2Result.get(0));
		assertEquals(KVUtils.kv("SEDATELY", 1), p2Result.get(7));
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
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<Entry<Integer, List<String>>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check		
		List<Entry<Integer, List<String>>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("2=[BETWEEN]", p1Result.get(0).toString());
		
		List<Entry<Integer, List<String>>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("1=[ANOTHER, APPROACHED, CALASAREIGNE, DOUBLED, FORERUNNER, HAPPENED, IDLERS,, INSTINCT, ISLANDS;, MISFORTUNE, POMEGUE,, SEDATELY, SPANKER,, STRAIT,, TOPSAILS,, VOLCANIC]", p2Result.get(0).toString());
	}
	
	@Test
	public void reduceSource() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.reduce(String::concat)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());

		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertTrue(p1Result.get(0).startsWith("The ship drew on and had safely passed the strait, which some volcanicshock has made between"));
	}
	
	@Test
	public void reduceAfterTransformation() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.reduce(String::concat)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());

		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertTrue(p1Result.get(0).startsWith("Theshipdrewonandhadsafelypassedthestrait,whichsomevolcanicshockhas"));
	}
	
	@Test
	public void reduceAfterShuffle() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.map(s -> s.toString())
				.reduce(String::concat)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());

		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertTrue(p1Result.get(0).startsWith("Pomegue,=1asked=1between=2board.=1drew=1evil,=1forerunner=1harbor"));
	}
	
	@Test
	public void countSource() throws Exception {
		Future<Stream<Stream<Long>>> resultFuture = DStream.ofType(String.class, "wc")
				.count()
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Long>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Long>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());

		List<Long> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertEquals(6L, (long)p1Result.get(0));
	}
	
	@Test
	public void countAfterTransformation() throws Exception {
		Future<Stream<Stream<Long>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.count()
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Long>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Long>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());

		List<Long> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertEquals(62L, (long)p1Result.get(0));
	}
	
	@Test
	public void countAfterShuffle() throws Exception {
		Future<Stream<Stream<Long>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.count()
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Long>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<Long>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());

		List<Long> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertEquals(49L, (long)p1Result.get(0));
	}
	
	@Test
	public void min() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.classify(word -> word)
				.min(StringUtils::compareLength)
			.executeAs(EXECUTION_NAME);
		
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertEquals("is", p1Result.get(0));
		
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(1, p2Result.size());
		assertEquals("of", p2Result.get(0));
	}
	
	@Test
	public void distinctSingleStage() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.distinct()
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		p1Result.stream().collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).values().forEach(count -> {
			if (count > 1){
				throw new IllegalStateException("value occures more then once");
			}
		});
		
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		p2Result.stream().collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).values().forEach(count -> {
			if (count > 1){
				throw new IllegalStateException("value occures more then once");
			}
		});
	}
	
	@Test
	public void distinctAfterShuffle() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.classify(word -> word.length())
				.distinct()
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		p1Result.stream().collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).values().forEach(count -> {
			if (count > 1){
				throw new IllegalStateException("value occures more then once");
			}
		});
		
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		p2Result.stream().collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).values().forEach(count -> {
			if (count > 1){
				throw new IllegalStateException("value occures more then once");
			}
		});
	}
	
	@Test
	public void minMaxSingleStage() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.max(StringUtils::compareLength)
			.executeAs(EXECUTION_NAME);
		
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(0, p1Result.size());
		
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(1, p2Result.size());
		assertEquals("Calasareigne", p2Result.get(0));
	}
	
	@Test
	public void minMaxAfterShuffle() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.classify(s -> s)
				.max(StringUtils::compareLength)
			.executeAs(EXECUTION_NAME);
		
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(1, p1Result.size());
		assertEquals("forerunner", p1Result.get(0));
		
		List<String> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals(1, p2Result.size());
		assertEquals("Calasareigne", p2Result.get(0));
	}
	
	@Test(expected=IllegalStateException.class)
	public void failCrossJoinOnMultiplePartitions() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two)
			.executeAs(EXECUTION_NAME);
		resultFuture.get();
	}
	
	@Test(expected=IllegalStateException.class)
	public void failJoinMultiPartitionWithTransformationsAndPredicate() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.map(s -> s.toLowerCase())
				.join(two.map(s -> s.toUpperCase())).on(t2 -> t2._1().split("\\s+")[0].equals(t2._2().split("\\s+")[2]))
			.executeAs(EXECUTION_NAME);
		resultFuture.get();
	}
	
	@Test
	public void twoWayJoinWithPredicateAndClassifier() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);
		DStream<String> two = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
				.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<Tuple2<String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check	
		List<Tuple2<String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("[2 Amazon, Jeff Bezos 2]", p1Result.get(0).toString());
		
		List<Tuple2<String, String>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("[3 Hortonworks, Tom McCuch 3]", p2Result.get(6).toString());
	}
	
	@Test
	public void fourWayJoinWithIntermediateTransformations() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);
		DStream<String> two = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);
		DStream<String> three = DStream.ofType(String.class, "three").classify(a -> a.split("\\s+")[0]);
		DStream<String> four = DStream.ofType(String.class, "four").classify(a -> a.split("\\s+")[0]);
		
		Future<Stream<Stream<Tuple4<String, String, String, String>>>> resultFuture = one
				.join(two)
				.filter(t2 -> t2._1().contains("Hortonworks"))
				.map(t2 -> tuple2(t2._1().toUpperCase(), t2._2().toUpperCase()))
				.join(three)
				.join(four).on(t3 -> {
					String v1 = t3._1()._1().split("\\s+")[0];
					String v2 = t3._1()._2().split("\\s+")[2];
					String v3 = t3._2().split("\\s+")[0];
					String v4 = t3._3().split("\\s+")[0];
					return v1.equals(v2) && v1.equals(v3) && v1.equals(v4);
				 })
				 .map(t3 -> tuple4(t3._1()._1(), t3._1()._2(), t3._2(), t3._3()))
				.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple4<String, String, String, String>>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<Tuple4<String, String, String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Tuple4<String, String, String, String>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("[3 HORTONWORKS, ARUN MURTHY 3, 3 $1B, 3 5470 Great America Parkway Santa Clara, CA 95054]", p2Result.get(0).toString());
		
		assertTrue(resultPartitionsList.get(0).collect(Collectors.toList()).isEmpty());
	}
	
	@Test
	public void simpleTwoWayUnionWithClassifier() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one").classify(a -> a.split("\\s+")[0]);
		DStream<String> two = DStream.ofType(String.class, "two").classify(a -> a.split("\\s+")[2]);
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
//		ExecutionResultUtils.printResults(resultPartitionsStream, true);
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("2 Amazon", p1Result.get(0));
		assertEquals("Jeff Bezos 2", p1Result.get(1));
		assertEquals("Jeffrey Blackburn 2", p1Result.get(2));
	}
}
