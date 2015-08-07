package dstream.function;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import dstream.function.SerializableFunctionConverters.SerFunction;
import dstream.function.SerializableFunctionConverters.SerPredicate;

/**
 * Base implementation of {@link SerFunction} for multi-stream processing.
 * See {@link StreamJoinerFunction} and {@link StreamUnionFunction} for concrete 
 * implementation.
 *
 */
public abstract class AbstractMultiStreamProcessingFunction implements SerFunction<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -7336517082191905937L;

	protected List<Object[]> checkPointProcedures = new ArrayList<>();
	
	private int streamCounter = 1;
	
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
		this.streamCounter += joiningStreamsCount;
		Object[] procedure = new Object[2];
		procedure[0] = this.streamCounter;
		this.checkPointProcedures.add(procedure);
	}
	
	/**
	 * Will add transformation ({@link SerFunction}) or predicate ({@link SerPredicate}) to be 
	 * applied to the last (current) checkpoint.<br>
	 * To ensure that both (transformation and predicate) cold be represented as a {@link SerFunction}, 
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
	 * Will add transformation ({@link SerFunction}) or predicate ({@link SerPredicate}) to be 
	 * applied to the last (current) checkpoint.
	 *  
	 * @param transformationOrPredicate
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void addTransformationOrPredicate(SerFunction transformationOrPredicate) {
		Object[] currentProcedure = this.checkPointProcedures.get(this.checkPointProcedures.size()-1);
		
		SerFunction func = (SerFunction) currentProcedure[1] == null 
				? transformationOrPredicate 
						: transformationOrPredicate.compose((SerFunction) currentProcedure[1]);
		
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
