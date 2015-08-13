package dstream.local.ri;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import dstream.DStream;
import dstream.utils.KVUtils;
import dstream.utils.Tuples.Tuple2;
import dstream.utils.Tuples.Tuple3;

public class LocalDStreamExecutionTests {
	
	private static String EXECUTION_NAME = LocalDStreamExecutionTests.class.getSimpleName();
	
	@Test
	public void execute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(3, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("The ship drew on and had safely passed the strait, which some volcanic", p1Result.get(0));
		assertEquals("the forerunner of evil, asked one another what misfortune could have", p1Result.get(1));
	}
	
	@Test
	public void groupSource() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.group(record -> record.substring(0, 1))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
	}
	
	@Test
	public void groupAfterTransformation() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toUpperCase())
				.group(record -> record.substring(0, 1))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("HAPPENED ON BOARD.", p1Result.get(0));
	}
	
	@Test
	public void groupAfterShuffle() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toUpperCase())
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.group(entry -> entry.getKey().substring(0, 1))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();

		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
	}
	
	@Test
	public void transformationOnly() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.map(s -> s.toUpperCase())
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(3, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("THE FORERUNNER OF EVIL, ASKED ONE ANOTHER WHAT MISFORTUNE COULD HAVE", p1Result.get(0));
		assertEquals("THE SHIP DREW ON AND HAD SAFELY PASSED THE STRAIT, WHICH SOME VOLCANIC", p1Result.get(1));
	}
	
	@Test
	public void computeOnly() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
			.executeAs(EXECUTION_NAME);

		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(3, resultPartitionsList.size());
		
//		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("THE FORERUNNER OF EVIL, ASKED ONE ANOTHER WHAT MISFORTUNE COULD HAVE", p1Result.get(0));
		assertEquals("THE SHIP DREW ON AND HAD SAFELY PASSED THE STRAIT, WHICH SOME VOLCANIC", p1Result.get(1));
	}
	
	@Test
	public void computeCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("CINACLOV EMOS HCIHW ,TIARTS EHT DESSAP YLEFAS DAH DNA NO WERD PIHS EHT", p1Result.get(0));
	}
	
	@Test
	public void computeShuffleCompute() throws Exception {
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "wc")
				.<String>compute(stream -> stream.map(s -> s.toUpperCase()))
				.reduceValues(s -> s, s -> 1, Integer::sum)
				.<String>compute(stream -> stream.map(s -> new StringBuilder(s.getKey()).reverse().toString()))
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();

		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(3, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("SI HCIHW TCNITSNI TAHT HTIW ,SRELDI EHT TAHT NEEWTEB YLETADES DNA YLWOLS OS", p1Result.get(0));
	}
	
	@Test
	public void shuffleOnly() throws Exception {
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
				.reduceValues(s -> s, s -> 1, Integer::sum)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<String, Integer>>> resultPartitionsStream = resultFuture.get();
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
		List<Stream<Entry<String, Integer>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		// spot check
		List<Entry<String, Integer>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals(KVUtils.kv("CALASAREIGNE", 1), p1Result.get(0));
		assertEquals(KVUtils.kv("SEDATELY", 1), p1Result.get(3));
		
		List<Entry<String, Integer>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
		assertEquals(KVUtils.kv("APPROACHED", 1), p3Result.get(1));
		assertEquals(KVUtils.kv("BETWEEN", 2), p3Result.get(2));
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
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Entry<Integer, List<String>>>> resultPartitionsStream = resultFuture.get();
		List<Stream<Entry<Integer, List<String>>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(2, resultPartitionsList.size());
		
		// spot check
		List<Entry<Integer, List<String>>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("1=[POMEGUE,, INSTINCT, FORERUNNER, MISFORTUNE, SPANKER,]", p1Result.get(0).toString());
		assertEquals("2=[BETWEEN]", p1Result.get(1).toString());
		
		List<Entry<Integer, List<String>>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("1=[HAPPENED, CALASAREIGNE, SEDATELY, ISLANDS;, STRAIT,, DOUBLED, APPROACHED, TOPSAILS,, IDLERS,, VOLCANIC, ANOTHER]", p2Result.get(0).toString());
	}
	
	@Test
	public void simpleTwoWayCrossJoin() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
		List<Stream<Tuple2<String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		//spot check
		List<Tuple2<String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("[3 Hortonworks, Oleg Zhurakousky 3]", p1Result.get(0).toString());
		assertEquals("[2 Amazon, Rob Bearden 3]", p1Result.get(3).toString());
		
		List<Tuple2<String, String>> p2Result = resultPartitionsList.get(1).collect(Collectors.toList());
		assertEquals("[3 Hortonworks, Herb Cunitz 3]", p2Result.get(1).toString());
		assertEquals("[1 Oracle, Herb Cunitz 3]", p2Result.get(4).toString());
	}
	
	@Test
	public void twoWayJoinWithPredicate() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = one
				.join(two).on(tuple2 -> tuple2._1().substring(0, 1).equals(tuple2._2().substring(tuple2._2().length()-1)))
				.executeAs(EXECUTION_NAME);
		
		Stream<Stream<Tuple2<String, String>>> resultPartitionsStream = resultFuture.get();
		List<Stream<Tuple2<String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		// spot check	
		List<Tuple2<String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("[3 Hortonworks, Oleg Zhurakousky 3]", p1Result.get(0).toString());
		
		List<Tuple2<String, String>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
		assertEquals("[2 Amazon, Jeffrey Blackburn 2]", p3Result.get(1).toString());
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
		
		List<Stream<Tuple3<String, String, String>>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		// spot check
		List<Tuple3<String, String, String>> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("[3 Hortonworks, Oleg Zhurakousky 3, The ship drew on and had safely passed the strait, which some volcanic]", p1Result.get(0).toString());
		
		List<Tuple3<String, String, String>> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
		assertEquals("[2 Amazon, Jeffrey Blackburn 2, The ship drew on and had safely passed the strait, which some volcanic]", p3Result.get(1).toString());
		
	}
	
	@Test
	public void simpleTwoWayUnion() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");
		
		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
			.executeAs(EXECUTION_NAME);
		
		Stream<Stream<String>> resultPartitionsStream = resultFuture.get();
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("Arun Murthy 3", p1Result.get(0));
		assertEquals("Jeff Bezos 2", p1Result.get(1).toString());
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
		List<Stream<String>> resultPartitionsList = resultPartitionsStream.collect(Collectors.toList());
		assertEquals(4, resultPartitionsList.size());
		
		// spot check
		List<String> p1Result = resultPartitionsList.get(0).collect(Collectors.toList());
		assertEquals("Arun Murthy 3", p1Result.get(0));
		assertEquals("The ship drew on and had safely passed the strait, which some volcanic", p1Result.get(3).toString());
		
		List<String> p3Result = resultPartitionsList.get(2).collect(Collectors.toList());
		assertEquals("2 Amazon", p3Result.get(0));
		assertEquals("Tom McCuch 3", p3Result.get(1).toString());
		assertEquals("shock has made between the Calasareigne and Jaros islands; had doubled", p3Result.get(3).toString());
	}
}
