package org.apache.dstream.support;

import static org.apache.dstream.utils.Tuples.Tuple2.tuple2;
import static org.apache.dstream.utils.Tuples.Tuple3.tuple3;
import static org.apache.dstream.utils.Tuples.Tuple4.tuple4;
import static org.apache.dstream.utils.Tuples.Tuple5.tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.Function;
import org.apache.dstream.function.SerializableFunctionConverters.Predicate;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.apache.dstream.utils.Tuples.Tuple3;
import org.apache.dstream.utils.Tuples.Tuple4;
import org.apache.dstream.utils.Tuples.Tuple5;

/**
 * 
 *
 */
@SuppressWarnings("rawtypes")
public class Joiner implements Function<Stream<Stream<?>>, Stream<?>> {
	private static final long serialVersionUID = -4183663973773249859L;

	private boolean cached;
	
	private Stream leftStream;
	
	private Stream rightStream;
	
	private List result;
	
	private List cache;
	
	private Predicate[] predicates;
	
	private int iterationCounter;
	
	/**
	 * 
	 * @param predicates
	 */
	public Joiner(Predicate... predicates) {
		this.predicates = predicates;
	}
	
	/**
	 * 
	 */
	@Override
	public Stream<?> apply(Stream<Stream<?>> t) {
		return this.join(t);
	}
	
	/**
	 * 
	 * @param streams
	 * @return
	 */
	public Stream<?> join(Stream<?>... streams) {
		return this.apply(Stream.of(streams));
	}
	
	/**
	 * 
	 * @param streams
	 * @return
	 */
	public Stream<?> join(Stream<Stream<?>> streams) {
		cache = new ArrayList<>();

		streams.forEach(stream -> {
			stream = (Stream<?>) this.preProcessValue(stream);
			if (leftStream == null){
				leftStream = stream;
			}
			else {
				if (result == null){
					rightStream = stream;
					result = new ArrayList<>();
					processTwoStreams();
				}
				else {
					leftStream = Stream.of(result.toArray());
					result.clear();
					rightStream = stream;
					processTwoStreams();
				}
			}
		});
		return result.stream();
	}
	
	/**
	 * 
	 * @param result
	 */
	@SuppressWarnings("unchecked")
	private void processTwoStreams(){
		cache.clear();
		cached = false;
		
		rightStream.forEach(valB -> {
			if (cached){
				leftStream = cache.stream();
			}
			leftStream.forEach(valA -> {
				if (!cached){
					cache.add(valA);
				}
				Object values;
				
				if (valA instanceof Tuple5){
					throw new IllegalStateException("Joining on Tuple5 is not supported. PLease merge it to a smaller tuple or independent structure.");
				}
				else if (valA instanceof Tuple4){
					values = tuple5(((Tuple4)valA)._1, ((Tuple4)valA)._2, ((Tuple4)valA)._3, ((Tuple4)valA)._4, valB);
				} 
				else if (valA instanceof Tuple3){
					values = tuple4(((Tuple3)valA)._1, ((Tuple3)valA)._2, ((Tuple3)valA)._3, valB);
				} 
				else if (valA instanceof Tuple2){
					values = tuple3(((Tuple2)valA)._1, ((Tuple2)valA)._2, valB);
				} 
				else {
					values = tuple2(valA, valB);
				}
	
				if (this.passedPredicate(values)){
					result.add(values);
				}
			});
			cached = true;
		});
		iterationCounter++;
	}
	
	/**
	 * 
	 * @param v
	 * @return
	 */
	protected Object preProcessValue(Object v) {
		return v;
	}
	
	/**
	 * 
	 * @param values
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean passedPredicate(Object values){
		return iterationCounter < predicates.length ? predicates[iterationCounter].test(values) : true;
	}
}
