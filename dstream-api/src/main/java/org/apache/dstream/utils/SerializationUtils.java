package org.apache.dstream.utils;

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
