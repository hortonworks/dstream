package io.dstream.tez;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;

import io.dstream.DStream;

public class StreamAPIUnionTests extends BaseTezTests {

	private final String applicationName = this.getClass().getSimpleName();

	@After
	public void after(){
		clean(applicationName);
	}

	@Test
	public void failUnclassifiedUnion() throws Exception {
		DStream<String> one = DStream.ofType(String.class, "one");
		DStream<String> two = DStream.ofType(String.class, "two");

		Future<Stream<Stream<String>>> resultFuture = one
				.union(two)
				.executeAs(this.applicationName);

		try {
			resultFuture.get(1000, TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

}
