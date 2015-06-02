package org.apache.dstream;

import java.util.stream.Stream;

import org.apache.dstream.support.PipelineConfigurationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Implementation of {@link ExecutionDelegate} which returns {@link PipelineExecutionChain}.
 * Primary use is testing.
 */
public class ExecutionContextSpecificationValidationDelegate implements ExecutionDelegate {
	
	private final Logger logger = LoggerFactory.getLogger(ExecutionContextSpecificationValidationDelegate.class);

	@Override
	public Stream<Stream<?>>[] execute(PipelineExecutionChain... pipelineSpecification) {
		PipelineConfigurationHelper.loadExecutionConfig(pipelineSpecification[0].getName());
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
