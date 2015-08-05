package org.apache.dstream.function;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;

public class StreamUnionFunction implements Function<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -2955908820407886806L;
	
	private final boolean distinct;
	
	private List<Object[]> postJoinProcedures = new ArrayList<>();
	
	private int sCounter = 1;
	
	public StreamUnionFunction(boolean distinct){
		this.distinct = distinct;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Stream<?> apply(Stream<Stream<?>> streams) {	

		AtomicInteger ctr = new AtomicInteger(2); 
		
		Stream<?> unionizedStream = streams
				.map(this::preProcessStream)
				.reduce((lStream,rStream) -> {
					Stream<?> newStream = Stream.concat(lStream,rStream);
					int currentStreamIdx = ctr.getAndIncrement();
					for (int j = 0; j < postJoinProcedures.size(); j++) {
						Object[] postProc = postJoinProcedures.get(j);
						if ((Integer)postProc[0] == currentStreamIdx){
							Function f = (Function) postProc[1];
							if (f != null){
								newStream = (Stream) f.apply(newStream);
							}
						}
					}
					return newStream;
				}).get();
		
		if (this.distinct){
			unionizedStream = unionizedStream.distinct();
		}
		
		return unionizedStream;
	}

	/**
	 * Will add a check point at which additional functionality may be provided before 
	 * proceeding with the join.<br>
	 * For example:
	 * <pre>
	 * 	DStream[A].join(DStream2[B,C]).map(s -> . . .).join(DStream[D]).on(Predicate)
	 * </pre>
	 * The above represents 4-way join where upon joining of 3 streams 
	 * (1 + 2 = 3; DStream[A] - 1 and DStream[B,C] - 2) a mapping function needs to 
	 * be applied before proceeding with the fourth.<br>
	 * In the above case the value of 'joiningStreamsCount' is 2 at the first check 
	 * point and 1 at the second(last).
	 * 
	 * @param joiningStreamsCount
	 */
	public void addCheckPoint(int joiningStreamsCount){
		this.sCounter += joiningStreamsCount;
		Object[] procedure = new Object[2];
		procedure[0] = this.sCounter;
		this.postJoinProcedures.add(procedure);
	}
	
	/**
	 * Will add transformation ({@link Function}) or predicate ({@link Predicate}) to be 
	 * applied to the last (current) checkpoint.<br>
	 * To ensure that both (transformation and predicate) cold be represented as a {@link Function}, 
	 * this method will wrap provided 'transformationOrPredicate' with {@link DStreamToStreamAdapterFunction}.
	 * 
	 * @param operationName
	 * @param transformationOrPredicate
	 */
	public void addTransformationOrPredicate(String operationName, Object transformationOrPredicate) {
		DStreamToStreamAdapterFunction incomingFunc = new DStreamToStreamAdapterFunction(operationName, transformationOrPredicate);
		this.addTransformationOrPredicate(incomingFunc);
	}
	
	/**
	 * Will add transformation ({@link Function}) or predicate ({@link Predicate}) to be 
	 * applied to the last (current) checkpoint.
	 *  
	 * @param transformationOrPredicate
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addTransformationOrPredicate(Function transformationOrPredicate) {
		Object[] currentProcedure = this.postJoinProcedures.get(this.postJoinProcedures.size()-1);
		
		Function func = (Function) currentProcedure[1] == null 
				? transformationOrPredicate 
						: transformationOrPredicate.compose((Function) currentProcedure[1]);
		
		currentProcedure[1] = func;
	}
	
	/**
	 * Will allow sub-classes to provide additional logic to be applied on the {@link Stream} before 
	 * it is sent thru join functionality.
	 * 
	 * @param stream
	 * @return
	 */
	protected Stream<?> preProcessStream(Stream<?> stream) {
		return stream;
	}
}
