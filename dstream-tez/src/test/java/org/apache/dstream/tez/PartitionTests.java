package org.apache.dstream.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Test;

import dstream.DStream;
import dstream.DStream.DStream2.DStream2WithPredicate;
import dstream.utils.ExecutionResultUtils;
import dstream.utils.KVUtils;
import dstream.utils.Tuples.Tuple2;

public class PartitionTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
//	@Test
//	public void partitionDefault() throws Exception {	
//		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "partitionDefault")
//				.partition()
//				.executeAs(this.applicationName + "-default");
//		
//		Stream<Stream<String>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
//		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
//		resultStreams.get(0).forEach(System.out::println);
//		Assert.assertEquals(1, resultStreams.size());
//		result.close();
//	}
	
	
//	@Test
//	public void partitionDefaultWithKV() throws Exception {	
//		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "partitionDefault")
//				.map(s -> KVUtils.kv(s, 1))
//				.partition()
//				.executeAs(this.applicationName + "-default");
//		
//		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
//		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
//		resultStreams.get(0).forEach(System.out::println);
//		Assert.assertEquals(1, resultStreams.size());
//		result.close();
//	}
	
//	@Test
//	public void partitionSetSize() throws Exception {	
//		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "partitionSetSize")
//				.partition()
//				.executeAs(this.applicationName + "-size");
//		
//		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
//		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
//		Assert.assertEquals(4, resultStreams.size());
//		result.close();
//	}
	
//	@Test
//	public void partitionSetSizeMultiStages() throws Exception {	
//		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "partitionSetSizeMultiStages")
//				.filter(line -> line.length() > 70)
//				.partition()  
//				.flatMap(line -> Stream.of(line.split(" ")))
//				.reduceGroups(s -> s, s -> 1, Integer::sum)
//				.executeAs(this.applicationName + "-size");
//		
//		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
//		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
//		Assert.assertEquals(4, resultStreams.size());
//		result.close();
//	}

//	@Test
//	public void partitionSetSizeAndPartitioner() throws Exception {	
//		new File("TestPartitioner").delete();
//		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "partitionSetSizeAndPartitioner")
//				.partition()
//				.executeAs(this.applicationName + "-partitioner");
//		
//		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
//		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
//		Assert.assertEquals(6, resultStreams.size());
//		result.close();
//		assertTrue(new File("TestPartitioner").exists());
//	}
	
//	@Test
//	public void partitionAfterJoinDefault() throws Exception {	
//		DStream<String> s1 = DStream.ofType(String.class, "hash");
//		DStream<String> s2 = DStream.ofType(String.class, "probe");
//		
//		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = s1
//				.filter(s -> true)
//				.join(s2).on(a -> true)
//				.partition()
//				.executeAs(this.applicationName + "-default");
//		
//		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
//		List<Stream<Tuple2<String, String>>> resultStreams = result.collect(Collectors.toList());
//		Assert.assertEquals(1, resultStreams.size());
//		result.close();	
//	}
	
//	@Test
//	public void partitionAfterJoinSize() throws Exception {	
//		DStream<String> s1 = DStream.ofType(String.class, "hash");
//		DStream<String> s2 = DStream.ofType(String.class, "probe");
//		
//		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = s1
//				.filter(s -> true)
//				.join(s2).on(a -> true)
//				.partition()
//				.executeAs(this.applicationName + "-size");
//		
//		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
//		List<Stream<Tuple2<String, String>>> resultStreams = result.collect(Collectors.toList());
//		Assert.assertEquals(4, resultStreams.size());
//		result.close();	
//	}
	
	@Test
	public void partitionAfterJoinSizeAndPartitioner() throws Exception {	
		DStream<String> s1 = DStream.ofType(String.class, "hash");
		DStream<String> s2 = DStream.ofType(String.class, "probe");
		
		 Future<Stream<Stream<Tuple2<Entry<String, String>, Entry<String, String>>>>> resultFuture = s1
				.map(h -> KVUtils.kv(h.split(" ")[0], h))
				.join(s2.map(p -> KVUtils.kv(p.split(" ")[2], p))).on(t2 -> t2._1().getKey().equals(t2._2().getKey()))
//				.map(s -> {
//					System.out.println("ENTRY");
//					return  KVUtils.kv(s, 1);})
//				.group(s -> s)
				.executeAs(this.applicationName + "-partitioner");
		
		Stream result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
//		kl
		ExecutionResultUtils.printResults(result, true);
		
//		List<Stream<Entry<Tuple2<String, String>, Integer>>> resultStreams = result.collect(Collectors.toList());
//		Assert.assertEquals(6, resultStreams.size());
//		
//		List<String> rValues = resultStreams.get(0).map(s -> s.toString()).collect(Collectors.toList());
//		assertEquals(0, rValues.size());
//		
//		rValues = resultStreams.get(1).map(s -> s.toString()).collect(Collectors.toList());
//		assertEquals(0, rValues.size());
//		
//		rValues = resultStreams.get(2).map(s -> s.toString()).collect(Collectors.toList());
//		assertEquals(0, rValues.size());
//		
//		rValues = resultStreams.get(3).map(s -> s.toString()).collect(Collectors.toList());
//		assertEquals(2, rValues.size());
//		assertEquals("[3 Hortonworks, Jeffrey Blackburn 2]=1", rValues.get(0));
//		assertEquals("[1 Oracle, Herb Cunitz 3]=1", rValues.get(1));
//		
//		rValues = resultStreams.get(4).map(s -> s.toString()).collect(Collectors.toList());
//		assertEquals(0, rValues.size());
//		
//		rValues = resultStreams.get(5).map(s -> s.toString()).collect(Collectors.toList());
//		assertEquals(1, rValues.size());
//		assertEquals("[3 Hortonworks, Larry Ellison 1]=1", rValues.get(0));
		
		result.close();	
	}
	
	
	@Test
	public void partitionWithClassifierDefault() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "partitionWithClassifier")
				.filter(line -> line.length() > 73)
				.classify(s -> s.substring(0, 5))
				.executeAs(this.applicationName + "-default");
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		assertEquals(1, resultStreams.size());
		List<String> rValues = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(4, rValues.size());
		assertEquals("Chapter 61. How a Gardener May Get Rid of the Dormice that Eat His Peaches.", rValues.get(0));
		assertEquals("great d'Aguesseau, [*] M. de Villefort, to come, but have not much hope of", rValues.get(1));
		assertEquals("I wished to bury it during my whole life in my own bosom, but your brother", rValues.get(2));
		assertEquals("End of Project Gutenberg's The Count of Monte Cristo, by Alexandre Dumas, Pere", rValues.get(3));

		result.close();
	}
	
	@Test
	public void partitionWithClassifierSize() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = DStream.ofType(String.class, "partitionWithClassifier")
				.filter(line -> line.length() > 73)
				.classify(s -> s.substring(0, 5))
				.executeAs(this.applicationName + "-size");
		Stream<Stream<String>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		assertEquals(4, resultStreams.size());
		
		List<String> rValues = resultStreams.get(0).collect(Collectors.toList());
		assertEquals(1, rValues.size());
		assertEquals("Chapter 61. How a Gardener May Get Rid of the Dormice that Eat His Peaches.", rValues.get(0));
		
		rValues = resultStreams.get(1).collect(Collectors.toList());
		assertEquals(1, rValues.size());
		assertEquals("great d'Aguesseau, [*] M. de Villefort, to come, but have not much hope of", rValues.get(0));
		
		rValues = resultStreams.get(2).collect(Collectors.toList());
		assertEquals(2, rValues.size());
		assertEquals("I wished to bury it during my whole life in my own bosom, but your brother", rValues.get(0));
		assertEquals("End of Project Gutenberg's The Count of Monte Cristo, by Alexandre Dumas, Pere", rValues.get(1));
		
		rValues = resultStreams.get(3).collect(Collectors.toList());
		assertEquals(0, rValues.size());
		
		result.close();
	}
	
//	@Test
//	public void partitionAfterJoinWithClassifier() throws Exception {	
////		assertFalse(new File("TestPartitionerWithClassifier").exists());
//		DStream<Entry<String, Integer>> s1 = DStream.ofType(String.class, "partitionAfterJoinWithClassifier")
//				.filter(s -> s.length() > 60)
//				.flatMap(line -> Stream.of(line.split(" ")))
//				.map(s -> s.trim())
//				.reduceGroups(s -> s, s -> 1, Integer::sum);
//		
////		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = s1.executeAs(this.applicationName);
////		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get();
////		result.forEach(s -> s.forEach(System.out::println));
////		
////		System.out.println("===========");
//		
//		DStream<String> s2 = DStream.ofType(String.class, "bar")
//				.flatMap(line -> Stream.of(line.split(" ")))
//				.map(s -> s.trim());
//		
//		DStream<String> s3 = DStream.ofType(String.class, "bar");
//		
////		Future<Stream<Stream<String>>> resultFuture2 = s2.executeAs(this.applicationName);
////		Stream<Stream<String>> result2 = resultFuture2.get();
////		result2.forEach(s -> s.forEach(System.out::println));
//
////		Future<Stream<Stream<Tuple2<Entry<String, Integer>, String>>>> resultFuture = s1
////				.join(s2).on(a -> a._1().getKey().equals(a._2()))
//////				.aggregateGroups(s -> s, s -> s, Aggregators::aggregateFlatten)
//////				.partition(s -> s)
////				.executeAs(this.applicationName);
//		
//		s1
//				.join(s2).on(a -> a._1().getKey().equals(a._2()))
//				.reduceGroups(s -> {
//					System.out.println(s);
//					return s;}, s -> 1, Integer::sum)
//				.partition(s -> s)
//				.join(s3)
//				.executeAs(this.applicationName).get();
//		//resultFuture.get(1000000, TimeUnit.MILLISECONDS);
////		Stream<Stream<Entry<Tuple2<Entry<String, Integer>, String>, Integer>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
////		List<Stream<Entry<Tuple2<Entry<String, Integer>, String>, Integer>>> resultStreams = result.collect(Collectors.toList());
////		resultStreams.get(0).forEach(System.out::println);
////		System.out.println(resultStreams.size());
////		Assert.assertEquals(2, resultStreams.size());
////		result.close();	
////		assertFalse(new File("TestPartitionerWithClassifier").exists());
//	}
}
