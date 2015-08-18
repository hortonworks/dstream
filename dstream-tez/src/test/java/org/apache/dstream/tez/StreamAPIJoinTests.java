package org.apache.dstream.tez;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;

import dstream.DStream;
import dstream.utils.Tuples.Tuple2;

public class StreamAPIJoinTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void failUnclassifiedJoin() throws Exception {
		DStream<String> hash = DStream.ofType(String.class, "hash");
		DStream<String> probe = DStream.ofType(String.class, "probe");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = hash.join(probe).executeAs(this.applicationName);
		
		try {
			resultFuture.get(10000, TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}
}
