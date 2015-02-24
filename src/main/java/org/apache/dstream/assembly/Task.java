package org.apache.dstream.assembly;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;

public class Task<T,R> {
	private final SerializableFunction<Stream<T>, ?> function;
	
	public Task(SerializableFunction<Stream<T>, ?> function) {
		this.function = function;
	}

	@SuppressWarnings("unchecked")
	public <K,V> void execute(Stream<T> stream, ShuffleWriter<K, V> writer) {
		Object result = function.apply(stream);
		if (result instanceof Map){
			System.out.println("Result is Map: " + result);
			Set<Entry<K,V>> entry = ((Map<K,V>)result).entrySet();	
			entry.forEach(s -> writer.write(s.getKey(), s.getValue()));
		} else {
			System.out.println("Result is terminal");
			writer.write(null, (V) result);
		}
	}
	
}
