package org.apache.dstream;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;

class ComposableStreamFunction implements Function<Stream<?>, Stream<?>> {
	private final List<StreamFunction> streamOps;
	public ComposableStreamFunction(List<StreamFunction> streamOps){
		this.streamOps = streamOps;
	}

	private static final long serialVersionUID = -1496510916191600010L;

	@Override
	public Stream<?> apply(Stream<?> streamIn) {
		Stream resultStream = streamIn;
		
		Function<Stream, Stream> finalFunction = null;
		
		for (StreamFunction streamFunction : streamOps) {
			if (finalFunction == null){
				finalFunction = streamFunction;
			}
			else {
				finalFunction = (Function) finalFunction.andThen(streamFunction);
			}
		}
		
		return finalFunction.apply(streamIn);
		
//		Iterator<Object> streamOpsIter = this.streamOps.iterator();
//		while (streamOpsIter.hasNext()){
//			String operationName = (String) streamOpsIter.next();
//			Function operation = (Function) streamOpsIter.next();
//			if (operationName.equals("flatMap")){
//				resultStream = resultStream.flatMap(operation);
//			} 
//			else if (operationName.equals("map")){
//				resultStream = resultStream.map(operation);
//			}
////			else if (entry.getKey().equals("filter")){
////				resultStream = resultStream.filter((Predicate)entry.getValue());
////			}
//			else {
//				throw new IllegalArgumentException("Unsupported: " + operationName);
//			}
//		}
//		return resultStream;
	}

}
