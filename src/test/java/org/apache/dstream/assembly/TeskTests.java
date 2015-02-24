package org.apache.dstream.assembly;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.utils.SerializableFunction;
import org.junit.Test;

public class TeskTests {

	@Test
	public void test(){
		Stream<String> stream = this.<String>buildStream();
		Task<String, Map<String, Integer>> task = this.<String, Map<String, Integer>>deserilizeTask();
		
		MyShuffWriter<String, Integer> writer = new MyShuffWriter<String, Integer>();
		task.execute(stream, writer);
	}
	
	private <T> Stream<T> buildStream(){
		Stream<T> stream = (Stream<T>) Stream.of("hello world hello");
		return stream;
	}
	
	private <T,R> Task<T, R> deserilizeTask(){
		SerializableFunction<Stream<String>, Map<String, Integer>> function = new SerializableFunction<Stream<String>, Map<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Integer> apply(Stream<String> stream) {
				return stream.flatMap(s -> Stream.of(s.split("\\s+")))
						.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum));
			}
		};
		
		Task<String, Map<String, Integer>> task = new Task<String, Map<String, Integer>>(function);
		return (Task<T, R>) task;
	}
	
	public static class MyShuffWriter<K,V> implements ShuffleWriter<K, V> {

		@Override
		public void write(K key, V value) {
			System.out.println(key + " - " + value);
		}
		
	}
}
