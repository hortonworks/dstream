package org.apache.dstream.utils;

import java.io.Serializable;

public interface Tuples {

	public static class Tuple2<A,B> implements Serializable {
		private static final long serialVersionUID = -531050350923907643L;
		
		public final A _1;
		public final B _2;
		
		private Tuple2(A _1, B _2){
			this._1 = _1;
			this._2 = _2;
		}
		public static <A,B> Tuple2<A,B> tuple2(A _1, B _2) {
			return new Tuple2<A, B>(_1, _2);
		}
		
		@Override
		public String toString(){
			return "(" + _1 + ", " + _2 + ")"; 
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Tuple2){
				Tuple2<?,?> that = (Tuple2<?, ?>) obj;
				return this._1.equals(that._1) && this._2.equals(that._2);
			}
	        return false;
	    }
	}
	
	public class Tuple3<A,B,C> extends Tuple2<A,B>{
		private static final long serialVersionUID = 6633770055559481085L;
		
		public final C _3;
		
		private Tuple3(A _1, B _2, C _3) {
			super(_1, _2);
			this._3 = _3;
		}
		
		public static <A,B,C> Tuple3<A,B,C> tuple3(A _1, B _2, C _3) {
			return new Tuple3<A,B,C>(_1, _2, _3);
		}
		
		@Override
		public String toString(){
			return "(" + _1 + ", " + _2 + ", " + _3 + ")"; 
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Tuple3){
				Tuple3<?,?,?> that = (Tuple3<?,?,?>) obj;
				return this._1.equals(that._1) && this._2.equals(that._2) && this._3.equals(that._3);
			}
	        return false;
	    }
	}
	
	public class Tuple4<A,B,C,D> extends Tuple3<A,B,C>{
		private static final long serialVersionUID = 526562558336237478L;
		
		public final D _4;
		
		private Tuple4(A _1, B _2, C _3, D _4) {
			super(_1, _2, _3);
			this._4 = _4;
		}
		
		public static <A,B,C,D> Tuple4<A,B,C,D> tuple4(A _1, B _2, C _3, D _4) {
			return new Tuple4<A,B,C,D>(_1, _2, _3, _4);
		}
		
		@Override
		public String toString(){
			return "(" + _1 + ", " + _2 + ", " + _3 + ", " + _4 + ")"; 
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Tuple4){
				Tuple4<?,?,?,?> that = (Tuple4<?,?,?,?>) obj;
				return this._1.equals(that._1) && this._2.equals(that._2) && this._3.equals(that._3) && this._4.equals(that._4);
			}
	        return false;
	    }
	}
	
	public class Tuple5<A,B,C,D,E> extends Tuple4<A,B,C,D>{
		private static final long serialVersionUID = 526561558336237478L;
		
		public final E _5;
		
		private Tuple5(A _1, B _2, C _3, D _4, E _5) {
			super(_1, _2, _3, _4);
			this._5 = _5;
		}
		
		public static <A,B,C,D,E> Tuple5<A,B,C,D,E> tuple5(A _1, B _2, C _3, D _4, E _5) {
			return new Tuple5<A,B,C,D,E>(_1, _2, _3, _4,_5);
		}
		
		@Override
		public String toString(){
			return "(" + _1 + ", " + _2 + ", " + _3 + ", " + _4 + ", " + _5 + ")"; 
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Tuple5){
				Tuple5<?,?,?,?,?> that = (Tuple5<?,?,?,?,?>) obj;
				return this._1.equals(that._1) && this._2.equals(that._2) && this._3.equals(that._3) && this._4.equals(that._4) && this._5.equals(that._5);
			}
	        return false;
	    }
	}
}
