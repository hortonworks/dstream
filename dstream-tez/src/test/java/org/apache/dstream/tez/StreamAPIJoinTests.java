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
import org.apache.dstream.utils.KVUtils;
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
	public void join() throws Exception {
		DistributableStream<String> hashStream = DistributableStream.ofType(String.class, "hash");
		DistributableStream<String> probeStream = DistributableStream.ofType(String.class, "probe");
		
		DistributableStream<String> hash = hashStream.map(line -> line.toUpperCase()).filter(s -> true);
		
		DistributableStream<Entry<Integer, String>> probe = probeStream.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
	    }).filter(s -> true).reduce(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);
	
		
		Future<Stream<Stream<Entry<Integer, Pair<String, String>>>>> resultFuture = hash.join(probe, 
				l -> Integer.parseInt(l.substring(0, l.indexOf(" ")).trim()), l -> l.substring(l.indexOf(" ")).trim(), r -> r.getKey(), r -> r.getValue()
				).executeAs(this.applicationName);

		Stream<Stream<Entry<Integer, Pair<String, String>>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		
		List<Stream<Entry<Integer, Pair<String, String>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<Integer,  Pair<String, String>>> firstResultStream = resultStreams.get(0);
		
		
		List<Entry<Integer, Pair<String, String>>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(3, firstResult.size());
		
		assertEquals(firstResult.get(0), KVUtils.kv(1, Pair.of("ORACLE", "Thomas Kurian, Larry Ellison")));
		assertEquals(firstResult.get(1), KVUtils.kv(2, Pair.of("AMAZON", "Jeffrey Blackburn, Jeff Bezos")));
		assertEquals(firstResult.get(2), KVUtils.kv(3, Pair.of("HORTONWORKS", "Tom McCuch, Herb Cunitz, Rob Bearden, Arun Murthy, Oleg Zhurakousky")));
		
		result.close();
	}
	
}
