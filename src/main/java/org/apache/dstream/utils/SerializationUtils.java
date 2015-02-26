package org.apache.dstream.utils;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.dstream.utils.TypeAwareStreams.TypeAwareObjectInputStream;
import org.apache.dstream.utils.TypeAwareStreams.TypeAwareObjectOutputStream;

/**
 * Serialization utility
 *
 */
public class SerializationUtils {

	public static void serialize(Object value, OutputStream targetOutputStream) {
		TypeAwareObjectOutputStream taos = null;
		try {
			taos = new TypeAwareObjectOutputStream(targetOutputStream);
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
		TypeAwareObjectInputStream tais = null;
		try {
			tais = new TypeAwareObjectInputStream(sourceInputStream);
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
