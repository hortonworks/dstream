package dstream.support;

import dstream.utils.Tuples.Tuple;
import dstream.utils.Tuples.Tuple6;

public interface Tuple7<A,B,C,D,E,F,G> extends Tuple6<A, B, C, D, E, F> {
	@SuppressWarnings("unchecked")
	public static <A,B,C,D,E,F,G> Tuple7<A,B,C,D,E,F,G> tuple7(A _1, B _2, C _3, D _4, E _5, F _6, G _7) {
		return (Tuple7<A,B,C,D,E,F,G>) new Tuple(_1, _2, _3, _4, _5, _6, _7);
	}
	
	default A _7() {
		return ((Tuple)this).get(6);
	}
}
