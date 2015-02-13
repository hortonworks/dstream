package org.apache.dstream;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

public class StreamExecutionContextTests {

	@Test(expected=IllegalStateException.class)
	public void validateNoExecutionContextFound(){
		StreamExecutionContext.of(new MyStreamable<Object>());
	}
	
	@Test
	public void validateExecutionContextFound() throws Exception {
		URLClassLoader cl = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Method addUrlMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
		addUrlMethod.setAccessible(true);
		addUrlMethod.invoke(cl, new File("/Users/ozhurakousky/dev/dstream/test").toURI().toURL());
		Object executionContext = StreamExecutionContext.of(new MyStreamable<Object>());
		Assert.assertNotNull(executionContext);
		Assert.assertTrue(executionContext instanceof TestStreamExecutionContext);
	}
	
	
	private static class MyStreamable<T> implements Streamable<T> {
		@Override
		public Stream<T> toStream() {
			return null;
		}
	}
}
