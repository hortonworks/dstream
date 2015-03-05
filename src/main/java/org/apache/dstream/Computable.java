package org.apache.dstream;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;

/**
 * Strategy which defines a set of <i>compute operators</i>.
 * Each <i>compute operator</i> is a <b>carrier</b> operator and carries an instance of 
 * {@link SerializableFunction} to be applied on a local {@link Stream}.
 * <br>
 * Compute operators can be <i>intermediate</i> or <i>terminal</i>
 * <br>
 * The <i>intermediate</i> operators return an instance of {@link Distributable} allowing data 
 * distribution functionality to be added to the {@link DistributedPipeline} (e.g., combine(..), 
 * partition(..) etc.). These operators <i><b>will not</b></i> trigger execution of the pipeline.
 * To trigger the execution of the pipeline you would need to terminate it with
 * one of the <i>terminal</i> operators (see {@link Persistable}, {@link Computable}). 
 * <br>
 * The <i>terminal</i> operators will trigger the execution of the pipeline returning a single result.
 * 
 * @param <T>
 */
public interface Computable<T> {

	/**
	 * A <i>terminal</i> operator returning a result of type {@link Integer}
	 * 
	 * @param computeFunction {@link SerializableFunction} to be applied on localized {@link Stream}
	 * @return
	 */
	int computeInt(SerializableFunction<Stream<T>, Integer> computeFunction);
	
	/**
	 * A <i>terminal</i> operator returning a result of type {@link Long}
	 * 
	 * @param computeFunction {@link SerializableFunction} to be applied on localized {@link Stream}
	 * @return
	 */
	long computeLong(SerializableFunction<Stream<T>, Long> computeFunction);
	
	/**
	 * A <i>terminal</i> operator returning a result of type {@link Double}
	 * 
	 * @param computeFunction {@link SerializableFunction} to be applied on localized {@link Stream}
	 * @return
	 */
	double computeDouble(SerializableFunction<Stream<T>, Double> computeFunction);
	
	/**
	 * A <i>terminal</i> operator returning a result of type {@link Boolean}
	 * 
	 * @param computeFunction {@link SerializableFunction} to be applied on localized {@link Stream}
	 * @return
	 */
	boolean computeBoolean(SerializableFunction<Stream<T>, Boolean> computeFunction);

	/**
	 * An <i>intermediate</i> operator returning a result of type {@link Distributable} referencing the
	 * Key/Value pairs returned by the <i>computeFunction</i> and represented as {@link Entry} allowing 
	 * data distribution operators to be added.
	 * 
	 * @param computeFunction {@link SerializableFunction} to be applied on localized {@link Stream}
	 * @return
	 */
	<K,V> Distributable<K,V> computeMappings(SerializableFunction<Stream<T>, Map<K,V>> computeFunction);
}
