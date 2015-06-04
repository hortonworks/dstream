package org.apache.dstream.utils;

public class TypeUtils {

	@SuppressWarnings("unchecked")
	public static <T> T cast(Object o){
		return (T) o;
	}
}
