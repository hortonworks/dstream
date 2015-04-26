package org.apache.dstream;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributablePipelineSpecification.Stage;
import org.apache.dstream.support.DefaultHashPartitioner;
import org.junit.Test;

public class DistributablePipelineSpecificationBuilderTests {
	
	private String pipelineName = "pipeline-spec-validation";
	
	@SuppressWarnings("unchecked")
	@Test
	public void impliedStageWithoutOutputUri(){
		DistributablePipeline<Integer> pipeline = DistributablePipeline.ofType(Integer.class);
		Stream<?> result = pipeline.executeAs(this.pipelineName);
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(Integer.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void impliedStageWithoutOutputUriStream(){
		DistributableStream<Integer> pipeline = DistributableStream.ofType(Integer.class);
		Stream<?> result = pipeline.executeAs(this.pipelineName);
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(Integer.class));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void impliedStageWithOutputUri() throws Exception {
		DistributablePipeline<Map> pipeline = DistributablePipeline.ofType(Map.class);
		Stream<?> result = pipeline.executeAs(this.pipelineName, new URI("out"));
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(new URI("out"), pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(Map.class));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void impliedStageWithOutputUriStream() throws Exception {
		DistributableStream<Map> pipeline = DistributableStream.ofType(Map.class);
		Stream<?> result = pipeline.executeAs(this.pipelineName, new URI("out"));
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(new URI("out"), pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(Map.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void singleStage() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class);
		Stream<Stream<Object>> result = pipeline.<Object>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
		).executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(1, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void singleStageStream() throws Exception {
		DistributableStream<String> pipeline = DistributableStream.ofType(String.class);
		Stream<Stream<String>> result = pipeline
				.flatMap(line -> Stream.of(line.split(" ")))
				.filter(word -> true)
				.map(word -> word)
				.executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(1, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void twoStages() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class);
		Stream<Stream<Entry<String, Integer>>> result = pipeline.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).reduce(s -> s, s -> 1, Integer::sum)
		 .executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(2, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		Assert.assertNotNull(stage);
		Assert.assertNotNull(stage.getAggregatorOperator());
		Assert.assertNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_1", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void twoStagesStream() throws Exception {
		DistributableStream<String> pipeline = DistributableStream.ofType(String.class);
		Stream<Stream<Entry<String, Integer>>> result = pipeline
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
				.reduce(word -> word, word -> 1, Integer::sum)
				.executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(2, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		Assert.assertNotNull(stage);
		Assert.assertNotNull(stage.getAggregatorOperator());
		Assert.assertNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_1", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void twoStagesWithSubsequentCompute() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class);
		Stream<Stream<Entry<String, Integer>>> result = pipeline.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).reduce(s -> s, s -> 1, Integer::sum)
		 .compute(stream -> stream)
		 .executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(2, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		Assert.assertNotNull(stage);
		Assert.assertNotNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_1", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void singleStageWithTwoSubsequentComputes() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class);
		Stream<Stream<Object>> result = pipeline.<Object>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
		).compute(stream -> stream)
		.executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(1, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void twoStagesWithTwoSubsequentComputes() throws Exception {
		DistributablePipeline<String> pipeline = DistributablePipeline.ofType(String.class);
		Stream<Stream<Entry<String, Integer>>> result = pipeline.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).compute(stream -> stream)
		 .reduce(s -> s, s -> 1, Integer::sum)
		 .executeAs("pipeline-spec-validation");
		
		List<?> r = result.collect(Collectors.toList());
		List<Stream<?>> resultStreams = (List<Stream<?>>) r;
		Assert.assertEquals(1, resultStreams.size());
		Stream<DistributablePipelineSpecification> firstResultStream = (Stream<DistributablePipelineSpecification>) resultStreams.get(0);
		DistributablePipelineSpecification pipelineSpec = firstResultStream.findFirst().get();
		Assert.assertEquals(this.pipelineName, pipelineSpec.getName());
		Assert.assertEquals(2, pipelineSpec.getStages().size());
		Assert.assertNull(pipelineSpec.getOutputUri());
		
		Stage stage = pipelineSpec.getStages().get(0);
		Assert.assertNotNull(stage);
		Assert.assertNull(stage.getAggregatorOperator());
		Assert.assertNotNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_0", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
		
		stage = pipelineSpec.getStages().get(1);
		Assert.assertNotNull(stage);
		Assert.assertNotNull(stage.getAggregatorOperator());
		Assert.assertNull(stage.getProcessingFunction());
		Assert.assertEquals("STAGE_1", stage.getName());
		Assert.assertNull(stage.getSourceSupplier());
		Assert.assertTrue(stage.getPartitioner().getClass().isAssignableFrom(DefaultHashPartitioner.class));
		Assert.assertTrue(stage.getSourceItemType().isAssignableFrom(String.class));
	}

}
