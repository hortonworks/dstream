package org.apache.dstream.utils;

public class Assert {

	public static void numberGreaterThenZero(Number number) {
		if (number == null || number.intValue() == 0){
			throw new IllegalArgumentException("'number' must not be null and > 0");
		}
	}
}
