package org.apache.dstream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.SerializableHelpers.Function;
import org.apache.dstream.utils.Utils;
import org.junit.Test;

public class FunctionCompositionAndSerializationTests {

	
	@Test
	public void entryFromValue(){
		Stream<Entry<Long, String>> sourceStream = Stream.of(Utils.kv(0L, "Hello"));
		
		Stream<String> sourceStream2 = Stream.of("Oleg");
		
		Function<Stream<String>, ?> rootFunction = inStream -> inStream.map(word -> {
			System.out.println(word);
			return word.toUpperCase();}).toArray()[0];
		Function<Stream<Entry>, ?> entryFunction = inStream -> inStream.map(entry -> entry.getValue());
		
		Function deTypedRootFunction = rootFunction;
		
		deTypedRootFunction = deTypedRootFunction.compose(entryFunction);
		
//		deTypedRootFunction = (Function) entryFunction.andThen(deTypedRootFunction);
		

//		System.out.println(deTypedRootFunction.apply(sourceStream));
		
		
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(deTypedRootFunction);
			oos.close();
			byte[] arr = bos.toByteArray();
			
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(arr));
			deTypedRootFunction = (org.apache.dstream.SerializableHelpers.Function) ois.readObject();
			System.out.println(deTypedRootFunction.apply(sourceStream));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
}
