package org.apache.dstream;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.support.HashJoiner;
import org.junit.Test;

/**
 * This class is a continuation of {@link ExecutionContextSpecificationBuilderTests}
 * which strictly concentrates on validating proper assembly of join functionality.
 */
public class JoinSpecificationBuilderTests {

	@Test
	public void join() throws Exception {
//		DistributablePipeline<String> pipelineA = DistributablePipeline.ofType(String.class, "foo");
//		DistributablePipeline<String> pipelineB = DistributablePipeline.ofType(String.class, "bar");
//		
//		DistributablePipeline<Entry<String, Integer>> aFlow = pipelineA.<String>compute(stream -> stream
//				.flatMap(line -> Stream.of(line.split(" ")))
//				.map(word -> word)
//		).reduce(s -> s, s -> 1, Integer::sum);
//		
//		DistributablePipeline<Entry<String, Integer>> bFlow =  pipelineB.compute(stream -> stream
//				.flatMap(line -> Stream.of(line.split(" ")))
//		).reduce(s -> (String)s, s -> 1, Integer::sum);
//		
////		aFlow.join(bFlow, HashJoiner::join);
//		
//		aFlow.join(bFlow, l -> l.getKey(), l -> l.getValue(), r -> r.getKey(), r -> r.getValue()).executeAs("foo");
//		
//		System.in.read();
	}
}
