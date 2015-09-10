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
package io.dstream.utils;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Serialization utility
 *
 */
public class SerializationUtils {

	public static void serialize(Object value, OutputStream targetOutputStream) {
		ObjectOutputStream taos = null;
		try {
			taos = new ObjectOutputStream(targetOutputStream);
			taos.writeObject(value);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			try {
				taos.close();
			} catch (Exception e) {
				// ignore
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserialize(InputStream sourceInputStream, Class<T> objectType) {
		ObjectInputStream tais = null;
		try {
			tais = new ObjectInputStream(sourceInputStream);
			T result = (T) tais.readObject();
			return result;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			try {
				tais.close();
			} catch (Exception e) {
				// ignore
			}
		}
	}
}
