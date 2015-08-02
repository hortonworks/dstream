package org.apache.dstream.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.support.CollectionFactory;
import org.apache.dstream.utils.Assert;
import org.apache.dstream.utils.Tuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StreamJoinerFunction implements Function<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -3615487628958776468L;
	
	private final Logger logger = LoggerFactory.getLogger(StreamJoinerFunction.class);
	
	private CollectionFactory collectionFactory;
	
	private List<Object[]> postJoinProcedures = new ArrayList<>();
	
	private int sCounter = 1;
	
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
	 * 
	 */
	@Override
	public Stream<?> apply(Stream<Stream<?>> streams) {	
		Assert.notNull(streams, "'streams' must not be null");
		List<Stream<?>> streamsList = streams.map(this::preProcessStream).collect(Collectors.toList());
		Assert.isTrue(streamsList.size() >= 2, "There must be 2+ streams available to perform join. Was " + streamsList.size());
		return this.join(streamsList);
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
	
	/**
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Stream<?> join(List<Stream<?>> streams) {
		Stream joinedStream = streams.remove(0);
		
		int streamCount = 0;
		int streamProcessedCounter = 2;
		Object[] postJoinProcedure = null;
		do {
			if (this.postJoinProcedures.size() > 0){
				postJoinProcedure = this.postJoinProcedures.remove(0);
				streamCount = (int) postJoinProcedure[0];
			}
			joinedStream = this.doJoin(joinedStream, streams.remove(0));
			if (streamCount == streamProcessedCounter){
				Function<Stream, Stream> postJoinProcedureFunction = (Function) postJoinProcedure[1];
				if (postJoinProcedureFunction != null){
					joinedStream = postJoinProcedureFunction.apply(joinedStream);
				}
			}
			streamProcessedCounter++;
		} while (streams.size() > 0);
		
		return joinedStream;
	}
	
	/**
	 * 
	 */
	private Stream<?> doJoin(Stream<?> joinedStream, Stream<?> joiningStream) {
		if (this.collectionFactory == null){
			Iterator<CollectionFactory> sl = ServiceLoader
		            .load(CollectionFactory.class, ClassLoader.getSystemClassLoader()).iterator();
			
			this.collectionFactory = sl.hasNext() ? sl.next() : null;
			if (this.collectionFactory == null){
				throw new IllegalStateException("Failed to find '" + CollectionFactory.class.getName() + "' provider.");
			}
			else {
				if (logger.isInfoEnabled()){
					logger.info("Loaded CollectionFactory: " + this.collectionFactory.getClass().getName());
				}
			}
		}
		List<Object> joiningStreamCache = this.collectionFactory.newList();
		return joinedStream.flatMap(lVal -> {
			boolean cached = joiningStreamCache.size() > 0;
			Stream<?> _joiningStream = cached ? joiningStreamCache.stream() : joiningStream;
			return _joiningStream.map(rVal -> {
				if (!cached){
					joiningStreamCache.add(rVal);
				}
				return this.mergeValues(lVal, rVal);
			});
		});	
	}
	
	/**
	 * 
	 */
	private Tuple mergeValues(Object left, Object right) {
		Tuple current = left instanceof Tuple ? (Tuple)left : new Tuple(left);
		Tuple cloned = current.size() > 1 ? current.clone() : current;
		cloned.add(right);
		return cloned;
	}
}
