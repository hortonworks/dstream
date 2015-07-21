package org.apache.dstream;

import org.apache.dstream.support.SerializableFunctionConverters.Function;
import org.apache.dstream.utils.Pair;

public interface SortedDistributableStream<T> extends DistributableStream<T> {

	<K,R> DistributableStream<Pair<T, R>> sortMergeJoin(DistributableStream<R> streamR, Function<? super T, ? extends K> lClassifier, Function<? super R, ? extends K> rClassifier);
}
