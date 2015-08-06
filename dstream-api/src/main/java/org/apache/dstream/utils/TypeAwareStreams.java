/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dstream.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Utility which holds definitions of type-aware Input and Output streams used 
 * during serialization and de-serialization of Scala functions generated in REPL.
 */
public class TypeAwareStreams {
	
	private static final Logger logger = LoggerFactory.getLogger(TypeAwareStreams.class);
	
	/**
	 * Implementation of the {@link OutputStream} which serializes class definition of the serialized object
	 * if such object was loaded by a {@link ClassLoader} assignable from scala.tools.nsc.interpreter.AbstractFileClassLoader
	 * since definitions of classes loaded by the scala.tools.nsc.interpreter.AbstractFileClassLoader
	 * are all generated.
	 */
	public static class TypeAwareObjectOutputStream extends ObjectOutputStream {
		
		/**
		 * 
		 * @param out
		 * @throws IOException
		 */
		public TypeAwareObjectOutputStream(OutputStream out) throws IOException {
			super(out);
		}
		
		/**
		 * 
		 */
		@Override
		protected void annotateClass(Class<?> cl) throws IOException {
			if (logger.isTraceEnabled()){
				logger.trace("Checking if class serialization is required for " + cl.getName());
			}
						
			ClassLoader classLoader = cl.getClassLoader();
			boolean shouldSerializeClassDef = false; // here you can create condition for when to annotate with class info
			this.writeBoolean(shouldSerializeClassDef);
				
			if (shouldSerializeClassDef){
				if (logger.isDebugEnabled()){
					logger.debug("Serializing definition of " + cl);
				}
				String classAsPath = cl.getName().replace('.', '/') + ".class";
				InputStream is = classLoader.getResourceAsStream(classAsPath);
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				IOUtils.copy(is, bos);
				byte[] classBytes = bos.toByteArray();
				writeInt(classBytes.length);
			    write(classBytes);
			}
		}
	}
	
	/**
	 * Implementation of the {@link InputStream} which serializes class definition of the serialized object
	 * if such object was loaded by a {@link ClassLoader} assignable from scala.tools.nsc.interpreter.AbstractFileClassLoader
	 * since definitions of classes loaded by the scala.tools.nsc.interpreter.AbstractFileClassLoader
	 * are all generated.
	 */
	public static class TypeAwareObjectInputStream extends ObjectInputStream {
		
		/**
		 * 
		 */
		public TypeAwareObjectInputStream(InputStream in) throws IOException {
			super(in);
		}
		
		/**
		 * 
		 */
		@Override
		protected Class<?> resolveClass(ObjectStreamClass desc) {
			Class<?> clazz = null;
			try {
				if (this.readBoolean()){
					ClassLoader classLoader = this.getClass().getClassLoader();
					String name = desc.getName();
					int length = readInt();
					byte[] classBytes = new byte[length];
					readFully(classBytes);
					ByteReadingClassLoader brClassLoader = new ByteReadingClassLoader(classLoader, classBytes, name);
					clazz = brClassLoader.loadClass(name);
				}		
				else {
					clazz = super.resolveClass(desc);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return clazz;
		}
		
		/**
		 * Implementation of the {@link ClassLoader} which is capable to load specific class
		 * from its serialized bytes. Used primarily to deal with REPL generated classes.
		 */
		private static final class ByteReadingClassLoader extends ClassLoader {
			private final String name;
			private final byte[] classBytes;
			public ByteReadingClassLoader(ClassLoader classLoader, byte[] classBytes, String name) {
				super(classLoader);
				this.classBytes = classBytes;
				this.name = name;
			}
			
			/**
			 * 
			 */
			@Override
			protected Class<?> findClass(String className) throws ClassNotFoundException {
				if (this.name.equals(className)){
					return this.defineClass(name, this.classBytes, 0, this.classBytes.length);
				}
				else {
					return super.findClass(className);
				}
			}
		}
	}
}
