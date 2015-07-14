package org.apache.dstream.support;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HashPartitionerTests {

	@Test(expected=IllegalStateException.class)
	public void failWithLessThenOnePartitionSize(){
		new HashPartitioner<>(0);
	}
	
	@Test(expected=IllegalStateException.class)
	public void failWithLessThenOnePartitionSizeUpdate(){
		HashPartitioner<String> hp = new HashPartitioner<>(1);
		hp.updatePartitionSize(0);
	}
	
	@Test
	public void validatePartitioner(){
		HashPartitioner<String> hp = new HashPartitioner<>(4);
		assertEquals(4, hp.getPartitionSize());
		assertEquals((Integer)1, hp.apply("a"));
		assertEquals((Integer)2, hp.apply("b"));
		assertEquals((Integer)3, hp.apply("c"));
		assertEquals((Integer)0, hp.apply("d"));
	}
}
