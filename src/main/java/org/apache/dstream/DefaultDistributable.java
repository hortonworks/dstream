package org.apache.dstream;

import java.util.Map.Entry;

public class DefaultDistributable<K,V> extends AbstractDistributable<K, V> {
	
	private static final long serialVersionUID = -1103988311522595028L;

	protected DefaultDistributable(
			AbstractDataPipelineExecutionProvider<Entry<K, V>> executionContext) {
		super(executionContext);
	}
}
