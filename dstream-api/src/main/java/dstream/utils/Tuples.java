/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dstream.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Definition of Tuple structures. 
 */
public interface Tuples {
	
	/**
	 * 
	 */
	public static class Tuple implements Serializable, Comparable<Object>,
			Tuple2<Object, Object>,
			Tuple3<Object, Object, Object>,
			Tuple4<Object, Object, Object, Object>,
			Tuple5<Object, Object, Object, Object, Object>,
			Tuple6<Object, Object, Object, Object, Object, Object> {
		
		private static final long serialVersionUID = 3141806564307095593L;
		
		private final List<Object> values;
		
		public Tuple(Object... values){
			this.values = new ArrayList<>(values.length);
			this.add(values);
		}
		
		private Tuple(int initialSize){
			this.values = new ArrayList<>(initialSize);
		}
		
		@SuppressWarnings("unchecked")
		public <T> T get(int idx) {
			return (T) this.values.get(idx);
		}
		
		public void add(Object... values){
			for (Object value : values) {
				this.values.add(value);
			}
		}
		
		public int size(){
			return this.values.size();
		}
		
		public Tuple clone(){
			Tuple cloned = new Tuple(this.values.size());
			cloned.values.addAll(this.values);
			return cloned;
		}
		
		@Override
		public String toString(){
			return values.toString(); 
		}
		
		@Override
		public int hashCode(){
			int hashCode = this.values.hashCode();
			return hashCode;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Tuple){
				Tuple that = (Tuple) obj;
				return this.values.equals(that.values);
			}
	        return false;
	    }

		@Override
		public int compareTo(Object that) {
			if (this.equals(that)){
				return 0;
			}	
			return -1;
		}
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 */
	public static interface Tuple2<A,B>  {
		@SuppressWarnings("unchecked")
		public static <A,B> Tuple2<A,B> tuple2(A _1, B _2) {
			return (Tuple2<A,B>) new Tuple(_1, _2);
		}

		default A _1() {
			return ((Tuple)this).get(0);
		}
		
		default B _2() {
			return ((Tuple)this).get(1);
		}
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 */
	public interface Tuple3<A,B,C> extends Tuple2<A,B>{	
		@SuppressWarnings("unchecked")
		public static <A,B,C> Tuple3<A,B,C> tuple3(A _1, B _2, C _3) {
			return (Tuple3<A,B,C>) new Tuple(_1, _2, _3);
		}
		
		default C _3() {
			return ((Tuple)this).get(2);
		}
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 * @param <D>
	 */
	public interface Tuple4<A,B,C,D> extends Tuple3<A,B,C>{	
		@SuppressWarnings("unchecked")
		public static <A,B,C,D> Tuple4<A,B,C,D> tuple4(A _1, B _2, C _3, D _4) {
			return (Tuple4<A,B,C,D>) new Tuple(_1, _2, _3, _4);
		}
		
		default D _4() {
			return ((Tuple)this).get(3);
		}
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 * @param <D>
	 * @param <E>
	 */
	public interface Tuple5<A,B,C,D,E> extends Tuple4<A,B,C,D>{	
		@SuppressWarnings("unchecked")
		public static <A,B,C,D,E> Tuple5<A,B,C,D,E> tuple5(A _1, B _2, C _3, D _4, E _5) {
			return (Tuple5<A,B,C,D,E>) new Tuple(_1, _2, _3, _4, _5);
		}
		
		default E _5() {
			return ((Tuple)this).get(4);
		}
	}
	
	/**
	 * 
	 * @param <A>
	 * @param <B>
	 * @param <C>
	 * @param <D>
	 * @param <E>
	 * @param <F>
	 */
	public interface Tuple6<A,B,C,D,E,F> extends Tuple5<A,B,C,D,E>{	
		@SuppressWarnings("unchecked")
		public static <A,B,C,D,E,F> Tuple6<A,B,C,D,E,F> tuple6(A _1, B _2, C _3, D _4, E _5, F _6) {
			return (Tuple6<A,B,C,D,E,F>) new Tuple(_1, _2, _3, _4, _5, _6);
		}
		
		default F _6() {
			return ((Tuple)this).get(5);
		}
	}
}
