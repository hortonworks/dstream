package org.apache.dstream.function;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.SerFunction;

/**
 * Implementation of {@link SerFunction} which will union multiple streams
 * while applying user functionality at check points (see this{@link #addCheckPoint(int)}.
 */
public class StreamUnionFunction extends AbstractMultiStreamProcessingFunction {
	private static final long serialVersionUID = -2955908820407886806L;
	
	private final boolean distinct;
	
	/**
	 * Constructs this function.
	 * 
	 * @param distinct boolean signaling if union results should be distinct, 
	 *  essentially supporting the standard <i>union</i> and <i>unionAll</i> semantics.
	 */
	public StreamUnionFunction(boolean distinct){
		this.distinct = distinct;
	}

	/**
	 * 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Stream<?> apply(Stream<Stream<?>> streams) {	

		AtomicInteger ctr = new AtomicInteger(2); 
		
		Stream<?> unionizedStream = streams
				.map(this::preProcessStream)
				.reduce((lStream,rStream) -> {
					Stream<?> newStream = Stream.concat(lStream,rStream);
					int currentStreamIdx = ctr.getAndIncrement();
					for (int j = 0; j < checkPointProcedures.size(); j++) {
						Object[] postProc = checkPointProcedures.get(j);
						if ((Integer)postProc[0] == currentStreamIdx){
							SerFunction f = (SerFunction) postProc[1];
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
}
