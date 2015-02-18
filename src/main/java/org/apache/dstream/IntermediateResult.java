package org.apache.dstream;

import java.util.Map.Entry;
import java.util.function.BinaryOperator;

public interface IntermediateResult<T> extends Submittable<T> {

	public Submittable<T> partition(int partitionSize, BinaryOperator<T> mergeFunction);
}
