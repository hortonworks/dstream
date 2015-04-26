package org.apache.dstream;

import java.util.stream.Stream;

public class DistributablePipelineSpecificationValidationDelegate implements ExecutionDelegate {

	@Override
	public Stream<?>[] execute(DistributablePipelineSpecification pipelineSpecification) {
		System.out.println("Executing: " + pipelineSpecification);
		return new Stream[]{Stream.of(pipelineSpecification)};
	}

	@Override
	public Runnable getCloseHandler() {
		// TODO Auto-generated method stub
		return null;
	}

}
