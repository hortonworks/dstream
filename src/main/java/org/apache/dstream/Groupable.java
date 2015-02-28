package org.apache.dstream;

import java.util.Map.Entry;

public interface Groupable<K,V> {

	public Submittable<Entry<K,V>> groupByKey();
}
