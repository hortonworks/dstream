package org.apache.dstream;

import java.util.concurrent.Future;
import java.util.stream.Stream;

public class DstreamTestUtils {

	public static <T> PipelineExecutionChain[] extractPipelineSpecifications(Future<Stream<T>> resultFuture){
		return ((ExecutionContextSpecificationExtractor)resultFuture).getSpecification();
	}
	
	public static <T> PipelineExecutionChain extractFirstPipelineSpecification(Future<Stream<T>> resultFuture){
		return ((ExecutionContextSpecificationExtractor)resultFuture).getSpecification()[0];
	}
}
