package dstream.support;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import dstream.support.SharedReference;

public class SharedReferenceTests {

	@Test
	public void validateProviderInitialization(){
		SharedReference<Integer> sr = SharedReference.of(45);
		assertEquals((Integer)45, sr.get());
	}
	
}
