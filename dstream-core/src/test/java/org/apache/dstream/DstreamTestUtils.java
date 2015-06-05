package org.apache.dstream;

import java.util.concurrent.Future;
import java.util.stream.Stream;

public class DstreamTestUtils {

	public static <T> ExecutionSpec[] extractPipelineExecutionSpecs(Future<Stream<T>> resultFuture){
		return ((ExecutionSpecExtractor)resultFuture).getExecutionSpec();
	}
	
	public static <T> ExecutionSpec extractFirstPipelineExecutionSpec(Future<Stream<T>> resultFuture){
		return ((ExecutionSpecExtractor)resultFuture).getExecutionSpec()[0];
	}
}
