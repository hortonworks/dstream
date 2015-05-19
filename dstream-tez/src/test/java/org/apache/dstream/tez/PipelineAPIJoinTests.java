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

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.utils.KVUtils;
import org.apache.dstream.utils.Pair;
import org.junit.Test;

public class PipelineAPIJoinTests {
	
	private final String applicationName = this.getClass().getSimpleName();

	@Test
	public void join() throws Exception {
		DistributablePipeline<String> hashPipeline = DistributablePipeline.ofType(String.class, "hash");
		DistributablePipeline<String> probePipeline = DistributablePipeline.ofType(String.class, "probe");
		
		DistributablePipeline<String> hash = hashPipeline.compute(stream -> stream
				.map(line -> line.toUpperCase())
		);
		
		DistributablePipeline<Entry<Integer, String>> probe = probePipeline.<Entry<Integer, String>>compute(stream -> stream
				.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
				})
		).reduce(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);
		
		Future<Stream<Stream<Entry<Integer, Pair<String, String>>>>> resultFuture = hash.join(probe, 
				hashElement -> Integer.parseInt(hashElement.substring(0, hashElement.indexOf(" ")).trim()), 
				hashElement -> hashElement.substring(hashElement.indexOf(" ")).trim(), 
				probeElement -> probeElement.getKey(), 
				probeElement -> probeElement.getValue()
			).executeAs(this.applicationName);

		Stream<Stream<Entry<Integer, Pair<String, String>>>> result = resultFuture.get(100000, TimeUnit.MILLISECONDS);
		
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
