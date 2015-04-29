package org.apache.dstream.support;

import java.io.Serializable;
import java.util.stream.Stream;

public class HashJoiner implements Serializable {
	private static final long serialVersionUID = 5957573722149836021L;

	public static <B,P,R> Stream<R> join(Stream<B> buildStream, Stream<P> probeStream) {
		return null;
	}
}
