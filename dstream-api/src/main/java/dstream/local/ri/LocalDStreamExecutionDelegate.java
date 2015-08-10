package dstream.local.ri;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import dstream.AbstractDStreamExecutionDelegate;
import dstream.DStreamInvocationPipeline;

/**
 * 
 * @param <T>
 */
public class LocalDStreamExecutionDelegate<T> extends AbstractDStreamExecutionDelegate<T> {
	
	private final List<Stream<?>> executableStreams = new ArrayList<>();
	
	@Override
	public Runnable getCloseHandler() {
		return new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
			}
		};
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Stream<Stream<?>> doExecute(String executionName, Properties executionConfig, DStreamInvocationPipeline... invocationPipelines) {
		for (DStreamInvocationPipeline invocationPipeline : invocationPipelines) {
			StreamBuilder streamBuilder = new StreamBuilder(executionName, invocationPipeline, executionConfig);
			Stream<?> exeStream = streamBuilder.build();
			this.executableStreams.add(exeStream);
		}
		
		if (this.executableStreams.size() == 1){
			List<Stream<?>> result = executableStreams.stream()
					.flatMap(stream -> ((Stream<Stream<?>>)stream))
					.map(stream -> stream.collect(Collectors.toList()).stream())
					.collect(Collectors.toList());
			
			return result.stream();
		}
		else {
			throw new IllegalStateException("Unsupported at the moment");
		}
	}
}
