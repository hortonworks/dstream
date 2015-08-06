package org.apache.dstream;

import org.junit.Test;

public class DStreamTests {
	
	private String streamName = DStreamOperationsCollectorTests.class.getSimpleName();

	@Test(expected=IllegalStateException.class)
	public void failWithSameStreamName(){
		DStream.ofType(String.class, "foo");
		DStream.ofType(String.class, "foo");
	}
	
	@Test
	public void sameNameAllowedAfterExec(){
		DStream.ofType(String.class, "foo").executeAs(this.streamName);
		DStream.ofType(String.class, "foo");
	}
}
