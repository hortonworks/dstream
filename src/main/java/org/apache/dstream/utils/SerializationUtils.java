package org.apache.dstream.utils;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

/**
 * Serialization utility
 *
 */
public class SerializationUtils {

	public static void serialize(Object object) {
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
}
