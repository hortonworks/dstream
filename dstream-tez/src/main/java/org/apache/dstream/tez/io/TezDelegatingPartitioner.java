package org.apache.dstream.tez.io;

import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import io.dstream.support.Classifier;



public class TezDelegatingPartitioner extends HashPartitioner {

	private static Classifier delegatingClassifier;

	public static void setDelegator(Classifier classifier){
		delegatingClassifier = classifier;
	}

	/**
	 *
	 */
	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		return this.doGetPartition((KeyWritable)key, (ValueWritable<?>)value, numPartitions);
	}

	/**
	 *
	 */
	private int doGetPartition(KeyWritable key, ValueWritable<?> value, int numPartitions) {
		int partitionId;
		Object valueToUse = key;
		if (key.getValue() == null){
			valueToUse = value.getValue();
		}
		else {
			valueToUse = key.getValue();
		}
		if (delegatingClassifier != null){
			partitionId = delegatingClassifier.getClassificationId(valueToUse);
		}
		else {
			partitionId = super.getPartition(valueToUse, null, numPartitions);
		}
		return partitionId;
	}
}
