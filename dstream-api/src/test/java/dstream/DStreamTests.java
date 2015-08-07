package dstream;

import org.junit.Test;

import dstream.DStream;

public class DStreamTests {
	
	private String streamName = DStreamOperationsCollectorTests.class.getSimpleName();

	@Test(expected=IllegalStateException.class)
	public void failWithSameStreamName(){
		DStream.ofType(String.class, "failWithSameStreamName");
		DStream.ofType(String.class, "failWithSameStreamName");
	}
	
	@Test
	public void sameNameAllowedAfterExec(){
		DStream.ofType(String.class, "sameNameAllowedAfterExec").executeAs(this.streamName);
		DStream.ofType(String.class, "sameNameAllowedAfterExec");
	}
}
