package org.apache.dstream;

import java.util.Map.Entry;

import org.apache.dstream.SerializableHelpers.BinaryOperator;
import org.apache.dstream.utils.Pair;
/**
 * Strategy which exposes additional functionality for Key/Value-based pipelines.
 *
 * @param <K>
 * @param <V>
 */
public interface DistributableKeyValuePipeline<K,V> extends Distributable<Entry<K,V>> {

	/**
	 * Will merge values of Key/Value pairs using provided 'valueMerger'
	 * 
	 * @param valueMerger
	 * @return
	 */
	DistributablePipeline<Entry<K, V>> reduceByKey(BinaryOperator<V> valueMerger);
	
	/**
	 * Will group values of Key/Value pairs into an array.  
	 * 
	 * @return
	 */
	DistributablePipeline<Entry<K, V[]>> groupByKey();
	
	/**
	 * 
	 * @param pipelineR
	 * @return
	 */
	<VJ> DistributableKeyValuePipeline<K, Pair<V, VJ>> joinByKey(DistributableKeyValuePipeline<K,VJ> pipelineR);
}
