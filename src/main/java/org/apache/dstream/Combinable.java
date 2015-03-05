package org.apache.dstream;

import java.util.Map.Entry;

import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.SerializableBinaryOperator;
import org.apache.dstream.utils.SerializableFunction;

/**
 * Strategy which exposes operations that are carriers of the <i>partitioning</i> specification 
 * together with the <i>combine-by-key</i> specification to be applied during or after the actual partitioning. 
 * The point at which <i>combine-by-key</i> specification is applied (during or after) is undefined since it depends on 
 * the implementation of shuffle/partition functionality of the target execution environment. 
 * 
 * @param <K> - 'key' type of Key/Value pairs
 * @param <V> - 'value' type of Key/Value pairs
 */
public interface Combinable<K,V> {
	
	/**
	 * An <i>intermediate</i> operation to supply instructions to partition the results of the computation 
	 * into 'N' partitions (N = <i>partitionSize</i>) using the default {@link Partitioner} provided by 
	 * the underlying execution environment and combining values of each key by applying <i>combineFunction</i>.
	 * <br>
	 * The <i>combineFunction</i> is a {@link SerializableBinaryOperator}&lt;V&gt; where 'V' corresponds 
	 * the <i>value</i> type of Key/Value pairs produced by the computation.
	 * 
	 * @param partitionSize
	 * @param combineFunction function that accepts two partial results and merges them
	 * @return an instance of {@link Persistable} allowing {@link DistributedPipeline} to be executed and materialized 
	 * into new {@link DistributedPipeline} or terminal value, representing the results of the distributed computation.
	 */
	public Persistable<Entry<K,V>> combine(int partitionSize, SerializableBinaryOperator<V> combineFunction);
	
	/**
	 * An <i>intermediate</i> operation to supply instructions to partition the results of the computation 
	 * using the provided {@link Partitioner} and combining values of each key by applying <i>combineFunction</i>.
	 * <br>
	 * The <i>combineFunction</i> is a {@link SerializableBinaryOperator}&lt;V&gt; where 'V' corresponds 
	 * the <i>value</i> type of Key/Value pairs produced by the computation.
	 * 
	 * @param partitioner {@link Partitioner} to be used to partition entries
	 * @param combineFunction function that accepts two partial results and merges them
	 * @return an instance of {@link Persistable} allowing {@link DistributedPipeline} to be executed and materialized 
	 * into new {@link DistributedPipeline} or terminal value, representing the results of the distributed computation.
	 */
	public Persistable<Entry<K,V>> combine(Partitioner<Entry<K,V>> partitioner, SerializableBinaryOperator<V> combineFunction);
	
	/**
	 * An <i>intermediate</i> operation to supply instructions to partition the results of the computation 
	 * using the provided {@link Partitioner} and combining values of each key by applying <i>combineFunction</i>.
	 * <br>
	 * The <i>combineFunction</i> is a {@link SerializableBinaryOperator}&lt;V&gt; where 'V' corresponds 
	 * the <i>value</i> type of Key/Value pairs produced by the computation.
	 * 
	 * @param partitionerFunction function to determine partition id
	 * @param combineFunction function that accepts two partial results and merges them
	 * @return an instance of {@link Persistable} allowing {@link DistributedPipeline} to be executed and materialized 
	 * into new {@link DistributedPipeline} or terminal value, representing the results of the distributed computation.
	 */
	public Persistable<Entry<K,V>> combine(SerializableFunction<Entry<K,V>, Integer> partitionerFunction, SerializableBinaryOperator<V> combineFunction);
}
