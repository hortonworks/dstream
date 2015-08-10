package org.apache.dstream.tez;

import java.io.File;

import dstream.function.HashGroupingFunction;

public class TestGrouper extends HashGroupingFunction{
	private static final long serialVersionUID = -1677894725281384687L;
	
	public TestGrouper(int partitionSize) {
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
	public Integer apply(Object input) {
		try {
			if (this.getClassifier() != null){
				File file = new File("TestPartitionerWithClassifier");
				file.createNewFile();
				file.deleteOnExit();
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return super.apply(input);
	}
}
