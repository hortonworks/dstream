package org.apache.dstream;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.SerializableLambdas.BinaryOperator;
import org.apache.dstream.SerializableLambdas.Function;
import org.apache.dstream.utils.Pair;

public interface CombinablePipeline<K,V> extends Pipeline<Entry<K,V>> {

	CombinablePipeline<K,V> combine(BinaryOperator<V> inputCombineFunction);
	
	CombinablePipeline<K,Iterable<V>> groupByKey();
	
	<VJ> CombinablePipeline<K,Pair<V, VJ>> join(CombinablePipeline<K,VJ> pipeline);
	
//	<V_2> MergablePipeline<K,Pair<V, V_2>> union(MergablePipeline<K,V_2> pipeline) {
//		return null;
//	}
	
	<KO,VO> CombinablePipeline<KO,VO> computeMappings(BinaryOperator<V> inputCombineFunction, 
            Function<? extends Stream<Entry<K,V>>, ? extends Stream<Entry<KO,VO>>> computeFunction);

	<KO,VO> CombinablePipeline<KO,VO> computeMappings(BinaryOperator<V> inputCombineFunction,
            Function<? extends Stream<Entry<K,V>>, ? extends Stream<Entry<KO,VO>>> computeFunction,
            BinaryOperator<VO> outputCombineFunction);
}
