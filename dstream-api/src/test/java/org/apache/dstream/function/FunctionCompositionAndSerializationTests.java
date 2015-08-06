package org.apache.dstream.function;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.function.SerializableFunctionConverters.SerFunction;
import org.apache.dstream.utils.KVUtils;
import org.junit.Assert;
import org.junit.Test;

public class FunctionCompositionAndSerializationTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void validateCompositionAndSerialization(){
		Stream<Entry<Long, String>> sourceStream = Stream.of(KVUtils.kv(0L, "Hello"));
		
		SerFunction<Stream<String>, ?> rootFunction = inStream -> inStream.map(word -> word.toUpperCase()).toArray()[0];
		SerFunction<Stream<Entry>, ?> entryFunction = inStream -> inStream.map(entry -> entry.getValue());
		
		SerFunction deTypedRootFunction = rootFunction;
		
		deTypedRootFunction = deTypedRootFunction.compose(entryFunction);
		
		deTypedRootFunction = this.serializeDeserialize(deTypedRootFunction);
		
		String result = (String) deTypedRootFunction.apply(sourceStream);
		Assert.assertEquals("HELLO", result);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void validateAndThenAndSerialization(){
		Stream<Entry<Long, String>> sourceStream = Stream.of(KVUtils.kv(0L, "Hello"));
		
		SerFunction<Stream<String>, ?> rootFunction = inStream -> inStream.map(word -> word.toUpperCase()).toArray()[0];
		SerFunction<Stream<Entry>, ?> entryFunction = inStream -> inStream.map(entry -> entry.getValue());
		
		SerFunction deTypedRootFunction = rootFunction;
		
		deTypedRootFunction = entryFunction.andThen(deTypedRootFunction);
		
		deTypedRootFunction = this.serializeDeserialize(deTypedRootFunction);
		
		String result = (String) deTypedRootFunction.apply(sourceStream);
		Assert.assertEquals("HELLO", result);
	}
	
	@SuppressWarnings("rawtypes")
	private SerFunction serializeDeserialize(SerFunction function){
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(function);
			oos.close();
			byte[] arr = bos.toByteArray();
			
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(arr));
			function = (org.apache.dstream.function.SerializableFunctionConverters.SerFunction) ois.readObject();
			return function;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
