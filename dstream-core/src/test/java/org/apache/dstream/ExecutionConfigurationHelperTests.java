package org.apache.dstream;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

public class ExecutionConfigurationHelperTests {

	@Test(expected=IllegalStateException.class)
	public void missingConfiguration(){
		ExecutionConfigurationHelper.loadExecutionConfig("foo");
	}
	
	@Test
	public void validConfiguration() {
		Properties prop = ExecutionConfigurationHelper.loadExecutionConfig("DStreamOperationsCollectorTests");
		assertTrue(prop.containsKey(DistributableConstants.DELEGATE));
		assertTrue(prop.containsKey(DistributableConstants.SOURCE + "foo"));
	}
}
