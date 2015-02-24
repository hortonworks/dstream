package org.apache.dstream.utils;

import java.util.Collection;

public class Assert {

	public static void numberGreaterThenZero(Number number) {
		if (number == null || number.intValue() == 0){
			throw new IllegalArgumentException("'number' must not be null and > 0");
		}
	}
	
	public static void notEmpty(Collection<?> collection) {
		if (collection == null || collection.size() == 0){
			throw new IllegalArgumentException("'collection' must not be null or empty");
		}
	}
	
	public static <T> void notEmpty(T[] array) {
		if (array == null || array.length == 0){
			throw new IllegalArgumentException("'array' must not be null or empty");
		}
	}
	
	public static <T> void notNull(T object) {
		notNull(object, "'object' must not be null");
	}
	
	public static <T> void notNull(T object, String message) {
		if (object == null){
			throw new IllegalArgumentException(message);
		}
	}
}
