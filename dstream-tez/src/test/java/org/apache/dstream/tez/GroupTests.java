package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributableStream;
import org.junit.After;
import org.junit.Test;

public class GroupTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}

	@Test
	public void streamGroup() throws Exception {	
		DistributableStream<String> sourcePipeline = DistributableStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, List<Integer>>>>> resultFuture = sourcePipeline
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.group(s -> s.getKey(), s -> s.getValue())
			 .executeAs(this.applicationName);
		
		Stream<Stream<Entry<String, List<Integer>>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, List<Integer>>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, List<Integer>>> firstResultStream = resultStreams.get(0);
		
		List<Entry<String, List<Integer>>> firstResult = firstResultStream.collect(Collectors.toList());
		System.out.println(firstResult);
//		Assert.assertEquals(10, firstResult.size());
//		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
//		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
//		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
		//assertEquals(3, this.bo.getTotalInvocations());
	}
	
}
