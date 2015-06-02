package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.DistributableStream;
import org.apache.dstream.utils.KVUtils;
import org.junit.After;
import org.junit.Test;

public class MapSideCombineTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}

	@Test
	public void computeReducePipeline() throws Exception {	
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, "computeReduce");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			).reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		System.out.println();
		Assert.assertEquals(10, firstResult.size());
		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
	}
	
	@Test
	public void computeReduceStream() throws Exception {	
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "computeReduce");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
	}
}
