package org.apache.dstream.assembly;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;
import org.apache.dstream.utils.Utils;

/**
 * 
 * @param <T>
 * @param <K>
 * @param <V>
 */
public class PreShuffleTask<T,K,V> extends Task<T, Entry<K,V>> {
	private static final long serialVersionUID = -5554288291945390620L;

	protected final SerializableFunction<Stream<T>, ?> function;
	/**
	 * 
	 * @param function
	 * @param preProcessFunction
	 */
	public PreShuffleTask(SerializableFunction<Stream<T>, ?> function, SerializableFunction<Stream<?>, Stream<?>> preProcessFunction) {
		if (preProcessFunction == null){
			this.function = function;
		} else {
			this.function = new SerializableFunction<Stream<T>, Map<K,V>>() {
				private static final long serialVersionUID = -1235381577031239367L;
				@Override
				public Map<K,V> apply(Stream<T> t) {
					@SuppressWarnings({"unchecked", "rawtypes"})
					Function<Stream<T>, Map<K,V>> f = (Function<Stream<T>, Map<K,V>>) function.compose((Function) preProcessFunction);
					return f.apply(t);
				}
			};
		}
	}

	/**
	 * 
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void execute(Stream<T> stream, Writer<Entry<K, V>> writer) {
		if (logger.isDebugEnabled()){
			logger.debug("Executing pre-shuffle task");
		}
		// executes user function
		Object result = this.function.apply(stream);
		//
		if (result instanceof Map){
			if (logger.isDebugEnabled()){
				logger.debug("Result is Map: " + result);
			}
			Set<Entry<K,V>> entry = ((Map<K,V>)result).entrySet();	
			entry.forEach(s -> writer.write(s));
		} else {
			if (logger.isDebugEnabled()){
				logger.debug("Result is terminal value: " + result);
			}
			writer.write((Entry<K, V>) Utils.toEntry(null, result));
		}
	}
}
