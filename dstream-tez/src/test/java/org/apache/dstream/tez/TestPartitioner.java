package org.apache.dstream.tez;

import java.io.File;

import org.apache.dstream.support.Partitioner;

public class TestPartitioner extends Partitioner<Object>{
	private static final long serialVersionUID = -1677894725281384687L;

	public TestPartitioner(int partitionSize) {
		super(partitionSize);
		try {
			File file = new File("TestPartitioner");
			file.createNewFile();
			file.deleteOnExit();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Integer apply(Object t) {
		return (t.hashCode() & Integer.MAX_VALUE) % this.getPartitionSize();
	}
}
