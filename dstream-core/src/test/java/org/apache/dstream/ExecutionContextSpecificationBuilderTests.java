package org.apache.dstream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.dstream.ExecutionSpec.Stage;
import org.apache.dstream.support.HashParallelizer;
import org.apache.dstream.support.UriSourceSupplier;
import org.apache.dstream.utils.KVUtils;
import org.junit.Test;

public class ExecutionContextSpecificationBuilderTests {
	
	private String pipelineName = "pipeline-spec-validation";
	
	private final String userDir = System.getProperty("user.dir");
	
	@Test(expected=IllegalStateException.class)
	public void failWithNoConfig() throws Exception {
		DistributablePipeline<Object> pipeline = DistributablePipeline.ofType(Object.class, "foo");
		pipeline.executeAs("fail-no-config");
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void failWithMissingSource() throws Exception {
		DistributablePipeline<Object> pipeline = DistributablePipeline.ofType(Object.class, "foo");
		pipeline.executeAs("fail-missing-source");
	}
	
	@Test(expected=IllegalStateException.class)
	public void failSchemeLessSource() throws Exception {
		DistributablePipeline<Object> pipeline = DistributablePipeline.ofType(Object.class, "foo");
		pipeline.executeAs("scheme-less-source");
	}
	
	@Test(expected=IllegalStateException.class)
	public void failSchemeLessOutput() throws Exception {
		DistributablePipeline<Object> pipeline = DistributablePipeline.ofType(Object.class, "foo");
		pipeline.executeAs("scheme-less-output");
	}
	
	@Test
	public void impliedStage() throws Exception {
		DistributablePipeline<Object> pipeline = DistributablePipeline.ofType(Object.class, "foo");
		Future<Stream<Stream<Object>>> resultFuture = pipeline.executeAs(this.pipelineName);
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		assertEquals(new URI("file:out"), pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(1, funcResult.length);
		assertEquals("foo bar", funcResult[0]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(Integer.class));
		
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		result.close();
	}
	
	@Test
	public void impliedStageStream() throws Exception {
		DistributableStream<Object> pipeline = DistributableStream.ofType(Object.class, "foo");
		Future<Stream<Stream<Object>>> resultFuture = pipeline.executeAs(this.pipelineName);
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(1, funcResult.length);
		assertEquals("foo bar", funcResult[0]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(Integer.class));
		
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		result.close();
	}

	@Test
	public void singleStage() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class, "foo");
		Future<Stream<Stream<Object>>> resultFuture = pipeline.<Object>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
		).executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals("foo", funcResult[0]);
		assertEquals("bar", funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		Stream<Stream<Object>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		result.close();
	}
	
	@Test
	public void singleStageStream() throws Exception {
		DistributableStream<String> pipeline = DistributableStream.ofType(String.class, "foo");
		Future<Stream<Stream<String>>> resultFuture = pipeline
				.flatMap(line -> Stream.of(line.split(" ")))
				.filter(word -> true)
				.map(word -> word)
				.executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals("foo", funcResult[0]);
		assertEquals("bar", funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		Stream<Stream<String>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		result.close();
	}
	
	@Test
	public void twoStages() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class, "foo");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipeline.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).reduce(s -> s, s -> 1, Integer::sum)
		 .executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals(KVUtils.kv("foo", 1), funcResult[0]);
		assertEquals(KVUtils.kv("bar", 1), funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		assertNotNull(stage);
		assertNotNull(stage.getAggregatorOperator());
		
		int aggrResult = (int) stage.getAggregatorOperator().apply(1, 5);
		assertEquals(6, aggrResult);
		
		assertNull(stage.getProcessingFunction());	
		assertEquals("1_foo", stage.getName());
		assertNull(stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(1000, TimeUnit.MILLISECONDS);
		result.close();
	}
	
	
	@Test
	public void twoStagesStream() throws Exception {
		DistributableStream<String> pipeline = DistributableStream.ofType(String.class, "foo");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipeline
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
				.reduce(word -> word, word -> 1, Integer::sum)
				.executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals(KVUtils.kv("foo", 1), funcResult[0]);
		assertEquals(KVUtils.kv("bar", 1), funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		assertNotNull(stage);
		assertNotNull(stage.getAggregatorOperator());
		
		int aggrResult = (int) stage.getAggregatorOperator().apply(1, 5);
		assertEquals(6, aggrResult);
		
		assertNull(stage.getProcessingFunction());
		assertEquals("1_foo", stage.getName());
		assertNull(stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@Test
	public void twoStagesWithSubsequentCompute() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class, "foo");
		Future<Stream<Stream<Entry<String, Long>>>> resultFuture = pipeline.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).reduce(s -> s, s -> 1, Integer::sum)
		 .<Entry<String, Long>>compute(stream -> stream
			    .map(entry -> KVUtils.kv(entry.getKey().toUpperCase(), new Long(entry.getValue())))
		).executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals(KVUtils.kv("foo", 1), funcResult[0]);
		assertEquals(KVUtils.kv("bar", 1), funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		assertNotNull(stage);
		assertNotNull(stage.getAggregatorOperator());
		
		int aggrResult = (int) stage.getAggregatorOperator().apply(1, 5);
		assertEquals(6, aggrResult);
		
		assertNotNull(stage.getProcessingFunction());
		
		funcResult = stage.getProcessingFunction().apply(Stream.of(KVUtils.kv("foo", 1))).toArray();
		assertEquals(1, funcResult.length);
		assertEquals(KVUtils.kv("FOO", 1L), funcResult[0]);
		
		assertEquals("1_foo", stage.getName());
		assertNull(stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@Test
	public void singleStageWithTwoSubsequentComputes() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class, "foo");
		Future<Stream<Stream<Object>>> resultFuture = pipeline.<Object>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
		).compute(stream -> stream)
		.executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals("foo", funcResult[0]);
		assertEquals("bar", funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@Test
	public void twoStagesWithTwoSubsequentComputes() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class, "foo");
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = pipeline.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).compute(stream -> stream)
		 .reduce(s -> s, s -> 1, Integer::sum)
		 .executeAs("pipeline-spec-validation");
		
		ExecutionSpec pipelineSpec = DstreamTestUtils.extractFirstPipelineExecutionSpec(resultFuture);
		
		Stage stage = pipelineSpec.getStages().get(0);
		assertNotNull(stage);
		assertNull(stage.getAggregatorOperator());
		assertNotNull(stage.getProcessingFunction());
		
		Object[] funcResult = stage.getProcessingFunction().apply(Stream.of("foo bar")).toArray();
		assertEquals(2, funcResult.length);
		assertEquals(KVUtils.kv("foo", 1), funcResult[0]);
		assertEquals(KVUtils.kv("bar", 1), funcResult[1]);
		
		assertEquals("0_foo", stage.getName());
		assertNotNull(stage.getSourceSupplier());
		assertEquals(new UriSourceSupplier(new URI("file:" + this.userDir + "/src/test/java/demo/monte-cristo.txt")), stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		assertNotNull(stage);
		assertNotNull(stage.getAggregatorOperator());
		
		int aggrResult = (int) stage.getAggregatorOperator().apply(1, 5);
		assertEquals(6, aggrResult);
		
		assertNull(stage.getProcessingFunction());
		assertEquals("1_foo", stage.getName());
		assertNull(stage.getSourceSupplier());
		assertTrue(stage.getParallelizer().getClass().isAssignableFrom(HashParallelizer.class));
		assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
}
