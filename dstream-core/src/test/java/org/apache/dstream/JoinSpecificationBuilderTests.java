package org.apache.dstream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.PipelineExecutionChain.Stage;
import org.apache.dstream.utils.Pair;
import org.junit.Test;

/**
 * This class is a continuation of {@link ExecutionContextSpecificationBuilderTests}
 * which strictly concentrates on validating proper assembly of join functionality.
 */
public class JoinSpecificationBuilderTests {

	@SuppressWarnings("unchecked")
	@Test
	public void validateJoinStructure() throws Exception {
		DistributablePipeline<String> pipelineA = DistributablePipeline.ofType(String.class, "foo");
		DistributablePipeline<String> pipelineB = DistributablePipeline.ofType(String.class, "bar");
		
		DistributablePipeline<Entry<String, Integer>> hash = pipelineA.<String>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> word)
		).reduce(s -> s, s -> 1, Integer::sum);
		
		DistributablePipeline<Entry<String, Integer>> probe =  pipelineB.compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(" ")))
		).reduce(s -> (String)s, s -> 1, Integer::sum);
		
		DistributablePipeline<Entry<String, Pair<Integer, Integer>>> joinedPipeline = 
				hash.join(probe, l -> l.getKey(), l -> l.getValue(), r -> r.getKey(), r -> r.getValue());
		
		Stage stage1 = ((List<Stage>)joinedPipeline).get(0);
		assertNull(stage1.getDependentExecutionContextSpec());
		Stage stage2 = ((List<Stage>)joinedPipeline).get(1);
		assertNull(stage2.getDependentExecutionContextSpec());
		Stage stage3 = ((List<Stage>)joinedPipeline).get(2);
		assertNotNull(stage3.getDependentExecutionContextSpec());
	}
}
