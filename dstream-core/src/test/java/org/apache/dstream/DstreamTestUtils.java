package org.apache.dstream;

import java.util.concurrent.Future;
import java.util.stream.Stream;

public class DstreamTestUtils {

	public static <T> ExecutionContextSpecification extractPipelineSpecification(Future<Stream<Stream<T>>> resultFuture){
		return ((ExecutionContextSpecificationExtractor)resultFuture).getSpecification();
	}
}
