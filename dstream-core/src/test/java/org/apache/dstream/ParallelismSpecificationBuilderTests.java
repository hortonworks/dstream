package org.apache.dstream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.dstream.ExecutionSpec.Stage;
import org.apache.dstream.support.HashParallelizer;
import org.junit.Test;

/**
 */
public class ParallelismSpecificationBuilderTests {
	
	@Test
	public void validateDefaultParallel() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "foo");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(1, stage.getParallelizer().getPartitionSize());
		assertNull(stage.getParallelizer().getClassifier());
	}
	
	@Test
	public void validateParallelWithInteger() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "foo");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.parallel(3).executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(3, stage.getParallelizer().getPartitionSize());
		assertNull(stage.getParallelizer().getClassifier());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateParallelWithParallelizer() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "foo");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.parallel(new HashParallelizer(5)).executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(5, stage.getParallelizer().getPartitionSize());
		assertNull(stage.getParallelizer().getClassifier());
	}

	@Test
	public void validateParallelWithParallelizerIntegerOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig3");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.parallel(7).executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(5, stage.getParallelizer().getPartitionSize());
		assertNull(stage.getParallelizer().getClassifier());
	}
	
	@Test
	public void validateParallelWithIntegerCompleteOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig4");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.parallel(7).executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(6, stage.getParallelizer().getPartitionSize());
		assertNull(stage.getParallelizer().getClassifier());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateParallelWithParallelizerCompleteOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig4");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.parallel(new HashParallelizer(8)).executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(6, stage.getParallelizer().getPartitionSize());
		assertNull(stage.getParallelizer().getClassifier());
	}
	
	@Test
	public void validateWithReduceInteger() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig1");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.reduce(s -> s, s -> 1, Integer::sum, 4)
				.executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(1, stage.getParallelizer().getPartitionSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(4, stage.getParallelizer().getPartitionSize());
	}
	
	@Test
	public void validateWithReduceIntegerConfigOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig2");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.reduce(s -> s, s -> 1, Integer::sum, 4)
				.executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(1, stage.getParallelizer().getPartitionSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(5, stage.getParallelizer().getPartitionSize());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateWithReduceParallelizer() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig1");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.reduce(s -> s, s -> 1, Integer::sum, new HashParallelizer(3))
				.executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(1, stage.getParallelizer().getPartitionSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(3, stage.getParallelizer().getPartitionSize());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateWithReduceParallelizerConfigOverrideInteger() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig3");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.reduce(s -> s, s -> 1, Integer::sum, new HashParallelizer(3))
				.executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(5, stage.getParallelizer().getPartitionSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(5, stage.getParallelizer().getPartitionSize());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateWithReduceParallelizerConfigCompleteOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig4");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.reduce(s -> s, s -> 1, Integer::sum, new HashParallelizer(3))
				.executeAs("ParallelismSpecificationBuilderTests");
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(6, stage.getParallelizer().getPartitionSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getParallelizer());
		assertTrue(stage.getParallelizer() instanceof HashParallelizer);
		assertEquals(6, stage.getParallelizer().getPartitionSize());
	}
}
