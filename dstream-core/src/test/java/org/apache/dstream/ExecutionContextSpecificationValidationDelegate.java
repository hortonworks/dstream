package org.apache.dstream;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Implementation of {@link ExecutionDelegate} which returns {@link PipelineExecutionChain}.
 * Primary use is testing.
 */
public class ExecutionContextSpecificationValidationDelegate implements ExecutionDelegate {
	
	private final Logger logger = LoggerFactory.getLogger(ExecutionContextSpecificationValidationDelegate.class);

	@SuppressWarnings("unchecked")
	@Override
	public Stream<Stream<?>>[] execute(PipelineExecutionChain... pipelineSpecification) {
		return Stream.of(pipelineSpecification)
				.map(v -> Stream.of(Stream.of(new Object())))
				.collect(Collectors.toList()).toArray(new Stream[]{});
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
