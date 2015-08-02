package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;
import static org.apache.dstream.utils.Tuples.Tuple2.tuple2;
import static org.apache.dstream.utils.Tuples.Tuple3.tuple3;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DStream;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.apache.dstream.utils.Tuples.Tuple3;
import org.junit.After;
import org.junit.Test;

public class StreamAPIJoinTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void simpleCrossJoin() throws Exception {
		DStream<String> hash = DStream.ofType(String.class, "hash");
		
		DStream<String> probe = DStream.ofType(String.class, "probe");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash.join(probe).executeAs(this.applicationName);
		
		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<Tuple2<String, String>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Tuple2<String, String>> firstResultStream = resultStreams.get(0);

		List<Tuple2<String, String>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(27, firstResult.size());
		
		// spot check
		assertEquals(firstResult.get(0), tuple2("1 Oracle", "Thomas Kurian 1"));
		assertEquals(firstResult.get(2), tuple2("1 Oracle", "Arun Murthy 3"));
		assertEquals(firstResult.get(5), tuple2("1 Oracle", "Jeff Bezos 2"));
		assertEquals(firstResult.get(11), tuple2("2 Amazon", "Arun Murthy 3"));
			
		result.close();
	}
	
	@Test
	public void joinTwoHashVProbeV() throws Exception {
		DStream<String> hash = DStream.ofType(String.class, "hash").map(line ->  line.toUpperCase());
		
		DStream<String> probe = DStream.ofType(String.class, "probe").map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b)
	      .map(entry -> entry.toString());
		
		Predicate<Tuple2<String, String>> p =  tuple2 -> Integer.parseInt(tuple2._1().substring(0, tuple2._1().indexOf(" ")).trim()) == Integer.parseInt(tuple2._2().split("=")[0].trim());

		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash.join(probe).on(p).executeAs(this.applicationName);
		
		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<Tuple2<String, String>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Tuple2<String, String>> firstResultStream = resultStreams.get(0);

		List<Tuple2<String, String>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), tuple2("1 ORACLE", "1=Thomas Kurian, Larry Ellison"));
		assertEquals(firstResult.get(1), tuple2("2 AMAZON", "2=Jeff Bezos, Jeffrey Blackburn"));
		assertEquals(firstResult.get(2), tuple2("3 HORTONWORKS", "3=Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy"));
		
		result.close();
	}
	
	@Test
	public void joinTwoHashVProbeKV() throws Exception {	
		DStream<String> hash = DStream.ofType(String.class, "hash").map(line ->  line.toUpperCase());
		
		DStream<Entry<Integer, String>> probe = DStream.ofType(String.class, "probe").map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);

		Predicate<Tuple2<String, Entry<Integer, String>>> p =  tuple2 -> Integer.parseInt(tuple2._1().substring(0, tuple2._1().indexOf(" ")).trim()) == tuple2._2().getKey();
		
		Future<Stream<Stream<Tuple2<String, Entry<Integer, String>>>>> resultFuture = hash.join(probe).on(p).executeAs(this.applicationName);
		
		Stream<Stream<Tuple2<String, Entry<Integer, String>>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<Tuple2<String, Entry<Integer, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Tuple2<String, Entry<Integer, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Tuple2<String, Entry<Integer, String>>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), tuple2("1 ORACLE", kv(1, "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), tuple2("2 AMAZON", kv(2, "Jeff Bezos, Jeffrey Blackburn")));
		assertEquals(firstResult.get(2), tuple2("3 HORTONWORKS", kv(3, "Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy")));
		
		result.close();
	}

	@Test
	public void joinTwoBothKV() throws Exception {
		DStream<Entry<Integer, String>> hash = DStream.ofType(String.class, "hash")
				.map(line -> kv(Integer.parseInt(line.split(" ")[0]), line.split(" ")[1]));
		
		DStream<Entry<Integer, String>> probe = DStream.ofType(String.class, "probe").map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);

		Predicate<Tuple2<Entry<Integer, String>, Entry<Integer, String>>> p =  tuple2 -> tuple2._1().getKey() == tuple2._2().getKey();
		
		Future<Stream<Stream<Tuple2<Entry<Integer, String>, Entry<Integer, String>>>>> resultFuture = hash.join(probe).on(p).executeAs(this.applicationName);
		
		Stream<Stream<Tuple2<Entry<Integer, String>, Entry<Integer, String>>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);

		List<Stream<Tuple2<Entry<Integer, String>, Entry<Integer, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Tuple2<Entry<Integer, String>, Entry<Integer, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Tuple2<Entry<Integer, String>, Entry<Integer, String>>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), tuple2(kv(1, "Oracle"), kv(1, "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), tuple2(kv(2, "Amazon"), kv(2, "Jeff Bezos, Jeffrey Blackburn")));
		assertEquals(firstResult.get(2), tuple2(kv(3, "Hortonworks"), kv(3, "Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy")));
		
		result.close();
	}
	
	@Test
	public void joinTwoBothKVWithContinuation() throws Exception {
		DStream<Entry<Integer, String>> hash = DStream.ofType(String.class, "hash")
				.map(line -> kv(Integer.parseInt(line.split(" ")[0]), line.split(" ")[1]));
		
		DStream<Entry<Integer, String>> probe = DStream.ofType(String.class, "probe").map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);

		Future<Stream<Stream<Entry<Integer, Tuple2<String, String>>>>> resultFuture = hash
			.join(probe)
			.on((Tuple2<Entry<Integer, String>,Entry<Integer, String>> tuple2) -> tuple2._1().getKey() == tuple2._2().getKey())
			.map(s -> kv(s._1().getKey(), tuple2(s._1().getValue(), s._2().getValue())))
			.executeAs(this.applicationName);
		
		Stream<Stream<Entry<Integer, Tuple2<String, String>>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);

		List<Stream<Entry<Integer, Tuple2<String, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<Integer, Tuple2<String, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Entry<Integer, Tuple2<String, String>>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), kv(1, tuple2("Oracle", "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), kv(2, tuple2("Amazon", "Jeff Bezos, Jeffrey Blackburn")));
		assertEquals(firstResult.get(2), kv(3, tuple2("Hortonworks", "Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy")));
		
		result.close();
	}
	
	@Test
	public void threeWayJoin() throws Exception {
		DStream<String> hashStream = DStream.ofType(String.class, "hash");
		DStream<String> probeStream = DStream.ofType(String.class, "probe");	
		DStream<Tuple2<String, Integer>> fooStream = DStream.ofType(String.class, "foo").map(s -> tuple2(s, s.hashCode()));
		
		DStream<String> hash = hashStream
				.map(line ->  line.toUpperCase());
		
		DStream<String> probe = probeStream.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b)
	      .map(entry -> entry.toString());

		Future<Stream<Stream<Tuple3<String, String, Tuple2<String, Integer>>>>> resultFuture = hash
				.join(probe)
				.on(tuple2 -> Integer.parseInt(tuple2._1().substring(0, tuple2._1().indexOf(" ")).trim()) == Integer.parseInt(tuple2._2().split("=")[0].trim()))
				.join(fooStream)
				.on(tuple3 -> tuple3._3()._2() < 0)
				.executeAs(this.applicationName);
		
		Stream<Stream<Tuple3<String, String, Tuple2<String, Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		
		List<Stream<Tuple3<String, String, Tuple2<String, Integer>>>> resultStreams = result.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Tuple3<String, String, Tuple2<String, Integer>>> firstResultStream = resultStreams.get(0);
		
		
		List<Tuple3<String, String, Tuple2<String, Integer>>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		System.out.println(firstResult);
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), tuple3("1 ORACLE", "1=Thomas Kurian, Larry Ellison", tuple2("we created them.", -1813742456)));
		assertEquals(firstResult.get(1), tuple3("2 AMAZON", "2=Jeff Bezos, Jeffrey Blackburn", tuple2("we created them.", -1813742456)));
		assertEquals(firstResult.get(2), tuple3("3 HORTONWORKS", "3=Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy", tuple2("we created them.", -1813742456)));
		result.close();
	}
	
	@Test
	public void threeWayJoinWithIntermitentContinuations() throws Exception {
		DStream<String> hashStream = DStream.ofType(String.class, "hash");
		DStream<String> probeStream = DStream.ofType(String.class, "probe");	
		DStream<Tuple2<String, Integer>> fooStream = DStream.ofType(String.class, "foo").map(s -> tuple2(s, s.hashCode()));
		
		DStream<String> hash = hashStream
				.map(line ->  line.toUpperCase());
		
		DStream<String> probe = probeStream.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b)
	      .map(entry -> entry.toString());

		Future<Stream<Stream<Tuple3<String, String, Tuple2<String, Integer>>>>> resultFuture = hash
				.join(probe)
				.on(tuple2 -> Integer.parseInt(tuple2._1().substring(0, tuple2._1().indexOf(" ")).trim()) == Integer.parseInt(tuple2._2().split("=")[0].trim()))
				.filter(s -> true)
				.join(fooStream)
				.on(tuple3 -> tuple3._3()._2() < 0)
				.executeAs(this.applicationName);
		
		Stream<Stream<Tuple3<String, String, Tuple2<String, Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		
		List<Stream<Tuple3<String, String, Tuple2<String, Integer>>>> resultStreams = result.peek(System.out::println).collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Tuple3<String, String, Tuple2<String, Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Tuple3<String, String, Tuple2<String, Integer>>> firstResult = firstResultStream.peek(System.out::println).collect(Collectors.toList());
		System.out.println(firstResult);
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), tuple3("1 ORACLE", "1=Thomas Kurian, Larry Ellison", tuple2("we created them.", -1813742456)));
		assertEquals(firstResult.get(1), tuple3("2 AMAZON", "2=Jeff Bezos, Jeffrey Blackburn", tuple2("we created them.", -1813742456)));
		assertEquals(firstResult.get(2), tuple3("3 HORTONWORKS", "3=Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy", tuple2("we created them.", -1813742456)));
		result.close();
	}
}
