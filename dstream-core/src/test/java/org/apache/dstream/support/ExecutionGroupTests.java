package org.apache.dstream.support;

import static org.apache.dstream.utils.KVUtils.kv;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.DstreamTestUtils;
import org.apache.dstream.ExecutionGroup;
import org.apache.dstream.ExecutionSpec;
import org.apache.dstream.utils.Pair;
import org.junit.Test;

public class ExecutionGroupTests {
	
	@Test(expected=IllegalArgumentException.class)
	public void validateEmptyPipelineArrayFailure(){
		ExecutionGroup.create("MyJobGroup");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void validateEmptyNameFailure(){
		ExecutionGroup.create(null);
	}
	
	@Test
	public void validateDuplicatPipelineDiscard(){
		DistributableStream<String> ds1 = DistributableStream.ofType(String.class, "ds1")
				.flatMap(line -> Stream.of(line.split(" ")));
		ExecutionGroup jg = ExecutionGroup.create("MyJobGroup", ds1, ds1, ds1);
		
		ExecutionSpec[] pipelineSpecs = DstreamTestUtils.extractPipelineExecutionSpecs(jg.executeAs("MyJobGroup"));
		
		assertEquals(1, pipelineSpecs.length);
	}

	@Test
	public void validateJobGroupStructure() throws Exception {
		DistributableStream<Entry<String, Integer>> ds1 = DistributableStream.ofType(String.class, "ds1")
				.flatMap(line -> Stream.of(line.split(" ")))
				.filter(word -> word.length() == 4)
				.combine(word -> word, word -> 1, Integer::sum);
		
		DistributableStream<String> ds2 = DistributableStream.ofType(String.class, "ds2")
				.filter(line -> line.length() <= 20);
		
		DistributableStream<Entry<String, Pair<Integer, Integer>>> ds3 = DistributableStream.ofType(String.class, "ds3")
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> kv(word, 1))
				.join(ds1, hash -> hash.getKey(), hash -> hash.getValue(), probe -> probe.getKey(), probe -> probe.getValue());
		
		ExecutionGroup jg = ExecutionGroup.create("MyJobGroup", ds1, ds2, ds3);
		
		Future<Stream<Stream<Stream<?>>>> resultFuture = jg.executeAs("MyJobGroup");
		
		ExecutionSpec[] pipelineSpecs = DstreamTestUtils.extractPipelineExecutionSpecs(resultFuture);
		
		assertEquals(3, pipelineSpecs.length);
		
		assertEquals("ds1", pipelineSpecs[0].getName());
		assertEquals("ds2", pipelineSpecs[1].getName());
		assertEquals("ds3", pipelineSpecs[2].getName());
		
		assertNull(pipelineSpecs[0].getOutputUri());
		assertNull(pipelineSpecs[1].getOutputUri());
		assertNull(pipelineSpecs[2].getOutputUri());
		
		assertEquals(2, pipelineSpecs[0].getStages().size());
		assertEquals(1, pipelineSpecs[1].getStages().size());
		assertEquals(2, pipelineSpecs[2].getStages().size());
		
		Stream<Stream<Stream<?>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		assertEquals(3, result.count());
	}
}
