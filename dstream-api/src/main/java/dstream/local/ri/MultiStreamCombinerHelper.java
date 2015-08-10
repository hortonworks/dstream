package dstream.local.ri;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import dstream.function.SerializableFunctionConverters.SerFunction;

public class MultiStreamCombinerHelper {

	private final SerFunction<Stream<Stream<?>>, Stream<?>> streamCombinerFunction;
	
	private List<Stream<?>> streams;
	
	public MultiStreamCombinerHelper(SerFunction<Stream<Stream<?>>, Stream<?>> streamCombinerFunction, Stream<?> rootStream) {
		this.streamCombinerFunction = streamCombinerFunction;
		this.streams = new ArrayList<>();
		this.streams.add(rootStream);
	}
	
	public void addStream(Stream<?> stream){
		this.streams.add(stream);
	}
	
	public Stream<?> invoke(){
		Stream<?> joinedStream = this.streamCombinerFunction.apply(this.streams.stream());
		return joinedStream;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getStreamCombinerFunction(){
		return (T) this.streamCombinerFunction;
	}
}
