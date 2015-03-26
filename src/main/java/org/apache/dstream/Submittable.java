package org.apache.dstream;

import java.util.stream.Stream;

public interface Submittable<T> {

	Stream<T> submit(String name);
}
