/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.function;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import dstream.SerializableAssets.SerFunction;
import dstream.utils.KVUtils;

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
			function = (dstream.SerializableAssets.SerFunction) ois.readObject();
			return function;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
