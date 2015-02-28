package org.apache.dstream.assembly;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableBinaryOperator;
import org.apache.dstream.utils.SerializableFunction;

/**
 * 
 * @param <T>
 * @param <K>
 * @param <V>
 */
public class PostShuffleTask<K,V> extends Task<Entry<K,V>, Entry<K,V>> {
	private static final long serialVersionUID = -2207289585205868330L;
	
	private final SerializableBinaryOperator<V> mergeFunction;

	/**
	 * 
	 * @param mergeFunction
	 * @param function
	 */
	public PostShuffleTask(SerializableBinaryOperator<V> mergeFunction, SerializableFunction<Stream<Entry<K,V>>, ?> function) {
		this.mergeFunction = mergeFunction;
	}

	@Override
	public void execute(Stream<Entry<K,V>> stream, Writer<Entry<K,V>> writer) {
		if (logger.isDebugEnabled()){
			logger.debug("Executing post-shuffle task");
		}
		stream.forEach(s -> writer.write(s));
	}
	
	/**
	 * 
	 * @return
	 */
	public SerializableBinaryOperator<V> getMergeFunction() {
		return mergeFunction;
	}
}
