package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.support.HashJoiner;
import org.junit.Test;

public class PipelineAPIJoinTests {
	
	private final String applicationName = this.getClass().getSimpleName();

	@Test
	public void join() throws Exception {
		DistributablePipeline<String> hashPipeline = DistributablePipeline.ofType(String.class, "hash");
		DistributablePipeline<String> probePipeline = DistributablePipeline.ofType(String.class, "probe");
		
		DistributablePipeline<Entry<Integer, String>> hash = hashPipeline.compute(stream -> stream
				.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[0]), split[1]);
				})
		);
		
		DistributablePipeline<Entry<Integer, String>> probe = probePipeline.<Entry<Integer, String>>compute(stream -> stream
				.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
				})
		).reduce(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);
	
		Future<Stream<Stream<Object>>> resultFuture = hash.join(probe, HashJoiner::join).executeAs(this.applicationName);
		
		resultFuture.get(1000000, TimeUnit.MILLISECONDS);
	}
}
