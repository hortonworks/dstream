package org.apache.dstream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SharedReferenceTests {

	@Test
	public void validateProviderInitialization(){
		SharedReference<Integer> sr = SharedReference.of(45);
		assertEquals((Integer)45, sr.get());
	}
	
}
