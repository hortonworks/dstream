package org.apache.dstream;

import java.util.stream.Stream;
/**
 * Implementation of {@link ExecutionDelegate} which returns {@link DistributablePipelineSpecification}.
 * Primary use is testing.
 */
public class DistributablePipelineSpecificationValidationDelegate implements ExecutionDelegate {

	@Override
	public Stream<?>[] execute(DistributablePipelineSpecification pipelineSpecification) {
		return new Stream[]{Stream.of(pipelineSpecification)};
	}

	@Override
	public Runnable getCloseHandler() {
		return null;
	}

}
