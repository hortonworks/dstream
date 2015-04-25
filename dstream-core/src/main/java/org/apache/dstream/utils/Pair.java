package org.apache.dstream.utils;

public class Pair<L,R> {
	private final L leftValue;

	private final R rightValue;
	
	public static <L,R> Pair<L,R> of(L leftValue, R rightValue) {
		return new Pair<L, R>(leftValue, rightValue);
	}
	public Pair(L leftValue, R rightValue) {
		this.leftValue = leftValue;
		this.rightValue = rightValue;
	}
	
	public L _1() {
		return leftValue;
	}

	public R _2() {
		return rightValue;
	}
}
