package org.apache.dstream;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.dstream.io.OutputSpecification;

public class StreamExecutionContext<T> {
	
	public static <T> StreamExecutionContext<T> of(Streamable<T> source) {
		return new StreamExecutionContext<T>();
	}
	
	public <K,V,R> Reducer<K,V> computeAsKeyValue(Class<K> outputKey, Class<V> outputVal, SerializableFunction<Stream<T>, Map<K,V>> function) {
		System.out.println("Serializing: " + function);
		System.out.println(Arrays.asList(function.getClass().getDeclaredFields()));
		System.out.println(Arrays.asList(function.getClass().getDeclaredMethods()));
		serialize(function);
		return null;
	}
	
	public InputStream toInputStream() {
		//http://stackoverflow.com/questions/22919013/inputstream-to-hadoop-sequencefile
		return null;
	}
	
	public Stream<T> toStream() {
		//http://stackoverflow.com/questions/22919013/inputstream-to-hadoop-sequencefile
		return null;
	}
	
	private static void serialize(Object object) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(bos);
			os.writeObject(object);
			os.close();
			System.out.println("Serialized pipeline size: " + bos.toByteArray().length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param <T>
	 * @param <R>
	 */
	public interface SerializableFunction<T,R> extends Function<T, R>, Serializable {}
	
	/**
	 * 
	 * @param <K>
	 * @param <V>
	 */
	public interface Reducer<K,V> {
		/**
		 * Will perform a post-shuffle reduce by key, producing the same Key/Value types as declared by 
		 * {@link IntermediateKVResult#computeAsKeyValue(Class, Class, SerializableFunction)} method.
		 * 
		 * @param mergeFunction
		 * @param reducers
		 * @return
		 */
		public IntermediateKVResult<Entry<K,V>> reduceByKey(BinaryOperator<V> mergeFunction, int reducers);
			
		public IntermediateKVResult<Entry<K,V>> reduceByValue(BinaryOperator<K> mergeFunction, int reducers);
		
		public IntermediateKVResult<Entry<K,V>> reduce(BinaryOperator<Entry<K,V>> mergeFunction, int reducers);
	}
	
	public interface IntermediateKVResult<T> {
		/**
		 * Defines a new compute stage on {@link Stream}. 
		 * 
		 * @param outputKey
		 * @param outputVal
		 * @param function
		 * @return
		 */
		public <K,V,R> Reducer<K,V> computeAsKeyValue(Class<K> outputKey, Class<V> outputVal, SerializableFunction<Stream<T>, Map<K,V>> function);
		
		public StreamExecutionContext<T> saveAs(OutputSpecification outtutSpec);
	}
	
}
