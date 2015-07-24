package org.apache.dstream.utils;

import java.io.Serializable;

public class Pair<L,R> implements Serializable {
	private static final long serialVersionUID = -2141290889431783473L;

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
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Pair){
			return ((Pair<?,?>)obj)._1().equals(this._1()) && ((Pair<?,?>)obj)._2().equals(this._2()); 
		} 
		else {
			return false;
		}
    }
	
	@Override
	public String toString(){
		return "([" + leftValue + "], [" + rightValue + "])";
	}
}
