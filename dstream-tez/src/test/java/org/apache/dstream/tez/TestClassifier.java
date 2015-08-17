package org.apache.dstream.tez;

import java.io.File;

import dstream.support.HashClassifier;

public class TestClassifier extends HashClassifier{
	private static final long serialVersionUID = -1677894725281384687L;
	
	public TestClassifier(int partitionSize) {
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
	public int doGetClassificationId(Object input) {
		try {

			if (this.getClassificationValueMapper() != null){
				File file = new File("TestPartitionerWithClassifier");
				file.createNewFile();
				file.deleteOnExit();
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return super.doGetClassificationId(input);
	}
}
