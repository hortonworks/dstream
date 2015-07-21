package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.utils.Pair;
import org.junit.After;
import org.junit.Test;

public class StreamAPIJoinTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void joinTwoHashVProbeKV() throws Exception {
		DistributableStream<String> hashStream = DistributableStream.ofType(String.class, "hash");
		DistributableStream<String> probeStream = DistributableStream.ofType(String.class, "probe");
		
		DistributableStream<String> hash = hashStream
				.map(line ->  line.toUpperCase());
		
		DistributableStream<Entry<Integer, String>> probe = probeStream.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);

		Future<Stream<Stream<Pair<String, Entry<Integer, String>>>>> resultFuture = hash.join(probe, 
				l -> Integer.parseInt(l.substring(0, l.indexOf(" ")).trim()), r -> r.getKey()
				).executeAs(this.applicationName);
		
		Stream<Stream<Pair<String, Entry<Integer, String>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		
		List<Stream<Pair<String, Entry<Integer, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Pair<String, Entry<Integer, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Pair<String, Entry<Integer, String>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), Pair.of("1 ORACLE", kv(1, "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), Pair.of("2 AMAZON", kv(2, "Jeff Bezos, Jeffrey Blackburn")));
		assertEquals(firstResult.get(2), Pair.of("3 HORTONWORKS", kv(3, "Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy")));
		
		result.close();
	}

	@Test
	public void joinTwoBothKV() throws Exception {
		DistributableStream<String> hashStream = DistributableStream.ofType(String.class, "hash");
		DistributableStream<String> probeStream = DistributableStream.ofType(String.class, "probe");
		
		DistributableStream<Entry<Integer, String>> hash = hashStream
				.map(line -> kv(Integer.parseInt(line.split(" ")[0]), line.split(" ")[1]));
		
		DistributableStream<Entry<Integer, String>> probe = probeStream.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);

		Future<Stream<Stream<Pair<Entry<Integer, String>, Entry<Integer, String>>>>> resultFuture = hash.join(probe, 
				l -> l.getKey(), r -> r.getKey()
				).executeAs(this.applicationName);
		
		Stream<Stream<Pair<Entry<Integer, String>, Entry<Integer, String>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);

		List<Stream<Pair<Entry<Integer, String>, Entry<Integer, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Pair<Entry<Integer, String>, Entry<Integer, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Pair<Entry<Integer, String>, Entry<Integer, String>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), Pair.of(kv(1, "Oracle"), kv(1, "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), Pair.of(kv(2, "Amazon"), kv(2, "Jeff Bezos, Jeffrey Blackburn")));
		assertEquals(firstResult.get(2), Pair.of(kv(3, "Hortonworks"), kv(3, "Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy")));
		
		result.close();
	}
	
	@Test
	public void joinTwoBothKVwithReduce() throws Exception {
		DistributableStream<String> hashStream = DistributableStream.ofType(String.class, "hash");
		DistributableStream<String> probeStream = DistributableStream.ofType(String.class, "probe");
		
		DistributableStream<Entry<Integer, String>> hash = hashStream
				.map(line -> kv(Integer.parseInt(line.split(" ")[0]), line.split(" ")[1]));
		
		DistributableStream<Entry<Integer, String>> probe = probeStream.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).reduceGroups(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);

		Future<Stream<Stream<Entry<Integer, Pair<String, String>>>>> resultFuture = hash.join(probe, 
				l -> l.getKey(), r -> r.getKey()
				).map(s -> kv(s._1().getKey(), Pair.of(s._1().getValue(), s._2().getValue())))
				.executeAs(this.applicationName);
		
		Stream<Stream<Entry<Integer, Pair<String, String>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);

		List<Stream<Entry<Integer, Pair<String, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<Integer, Pair<String, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Entry<Integer, Pair<String, String>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), kv(1, Pair.of("Oracle", "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), kv(2, Pair.of("Amazon", "Jeff Bezos, Jeffrey Blackburn")));
		assertEquals(firstResult.get(2), kv(3, Pair.of("Hortonworks", "Rob Bearden, Herb Cunitz, Tom McCuch, Oleg Zhurakousky, Arun Murthy")));
		
		result.close();
	}
}
