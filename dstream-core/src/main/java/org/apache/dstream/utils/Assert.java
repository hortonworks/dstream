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
		notEmpty(array, "'array' must not be null or empty");
	}
	
	public static <T> void notEmpty(T[] array, String message) {
		if (array == null || array.length == 0){
			throw new IllegalArgumentException(message);
		}
	}
	
	public static void notEmpty(String string, String message) {
		if (string == null || string.trim().length() == 0){
			throw new IllegalArgumentException(message);
		}
	}
	
	public static boolean isTrue(boolean _true){
		return isTrue(_true, "Result of boolean expression is not true");
	}
	
	public static boolean isTrue(boolean _true, String message){
		if (!_true){
			throw new IllegalStateException(message);
		}
		return true;
	}
	
	public static boolean isFalse(boolean _false){
		return isFalse(_false, "Result of boolean expression is not false");
	}
	
	public static boolean isFalse(boolean _false, String message){
		if (_false){
			throw new IllegalStateException(message);
		}
		return true;
	}
	
	public static void notEmpty(String string) {
		notEmpty(string, "'string' must not be null or empty");
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
