package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.utils.PipelineConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Implementation of {@link ExecutionDelegate} which returns {@link ExecutionContextSpecification}.
 * Primary use is testing.
 */
public class DistributablePipelineSpecificationValidationDelegate implements ExecutionDelegate {
	
	private final Logger logger = LoggerFactory.getLogger(DistributablePipelineSpecificationValidationDelegate.class);

	@Override
	public Stream<?>[] execute(ExecutionContextSpecification pipelineSpecification) {
		PipelineConfigurationUtils.loadExecutionConfig(pipelineSpecification.getName());
		return new Stream[]{Stream.of(new Object())};
	}

	@Override
	public Runnable getCloseHandler() {
		return new Runnable() {
			@Override
			public void run() {
				logger.info("Executing close handler");
			}
		};
	}

}
