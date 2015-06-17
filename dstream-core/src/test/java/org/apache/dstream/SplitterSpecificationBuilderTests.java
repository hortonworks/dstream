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
import org.apache.dstream.support.HashSplitter;
import org.junit.Test;

/**
 */
public class SplitterSpecificationBuilderTests {
	
	private String executionName = this.getClass().getSimpleName();
	
	@Test
	public void validateDefaultParallel() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "foo");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(1, stage.getSplitter().getSplitSize());
		assertNull(stage.getSplitter().getClassifier());
	}
	
	@Test
	public void validateParallelWithInteger() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "foo");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.split(3).executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(3, stage.getSplitter().getSplitSize());
		assertNull(stage.getSplitter().getClassifier());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateParallelWithParallelizer() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "foo");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.split(new HashSplitter(5)).executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(5, stage.getSplitter().getSplitSize());
		assertNull(stage.getSplitter().getClassifier());
	}

	@Test
	public void validateParallelWithParallelizerIntegerOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig3");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.split(7).executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(5, stage.getSplitter().getSplitSize());
		assertNull(stage.getSplitter().getClassifier());
	}
	
	@Test
	public void validateParallelWithIntegerCompleteOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig4");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.split(7).executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(6, stage.getSplitter().getSplitSize());
		assertNull(stage.getSplitter().getClassifier());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateParallelWithParallelizerCompleteOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig4");
		
		Future<Stream<Stream<String>>> resultFuture = pipelineA.split(new HashSplitter(8)).executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		List<Stage> stages = executionSpec.getStages();
		assertEquals(1, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(6, stage.getSplitter().getSplitSize());
		assertNull(stage.getSplitter().getClassifier());
	}
	
	@Test
	public void validateWithReduceInteger() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig1");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.combine(s -> s, s -> 1, Integer::sum, 4)
				.executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(1, stage.getSplitter().getSplitSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(4, stage.getSplitter().getSplitSize());
	}
	
	@Test
	public void validateWithReduceIntegerConfigOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig2");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.combine(s -> s, s -> 1, Integer::sum, 4)
				.executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(1, stage.getSplitter().getSplitSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(5, stage.getSplitter().getSplitSize());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateWithReduceParallelizer() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig1");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.combine(s -> s, s -> 1, Integer::sum, new HashSplitter(3))
				.executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(1, stage.getSplitter().getSplitSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(3, stage.getSplitter().getSplitSize());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateWithReduceParallelizerConfigOverrideInteger() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig3");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.combine(s -> s, s -> 1, Integer::sum, new HashSplitter(3))
				.executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(5, stage.getSplitter().getSplitSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(5, stage.getSplitter().getSplitSize());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void validateWithReduceParallelizerConfigCompleteOverride() throws Exception {
		DistributableStream<String> pipelineA = DistributableStream.ofType(String.class, "pConfig4");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipelineA
				.flatMap(line -> Stream.of(line.split("")))
				.combine(s -> s, s -> 1, Integer::sum, new HashSplitter(3))
				.executeAs(this.executionName);
		
		ExecutionSpec executionSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		List<Stage> stages = executionSpec.getStages();
		assertEquals(2, stages.size());
		
		Stage stage = stages.get(0);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(6, stage.getSplitter().getSplitSize());
		
		stage = stages.get(1);
		assertNotNull(stage.getSplitter());
		assertTrue(stage.getSplitter() instanceof HashSplitter);
		assertEquals(6, stage.getSplitter().getSplitSize());
	}
}
