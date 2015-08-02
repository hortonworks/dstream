package org.apache.dstream.tez;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DStream;
import org.apache.dstream.utils.Tuples.Tuple2;
import org.junit.After;
import org.junit.Test;

public class PartitionTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	@After
	public void after(){
		clean(applicationName);
	}
	
	@Test
	public void partitionDefault() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = 
				DStream.ofType(String.class, "default").partition().executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		result.close();
	}
	
	@Test
	public void partitionSetSize() throws Exception {	
		Future<Stream<Stream<String>>> resultFuture = 
				DStream.ofType(String.class, "wc").partition().executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(2, resultStreams.size());
		result.close();
	}

	@Test
	public void partitionSetSizeAndPartitioner() throws Exception {	
		assertFalse(new File("TestPartitioner").exists());
		Future<Stream<Stream<String>>> resultFuture = 
				DStream.ofType(String.class, "sAndP").partition().executeAs(this.applicationName);
		
		Stream<Stream<String>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<String>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(2, resultStreams.size());
		result.close();
		assertTrue(new File("TestPartitioner").exists());
	}
	
	@Test
	public void partitionAfterJoin() throws Exception {	
		DStream<String> s1 = DStream.ofType(String.class, "foo");
		DStream<String> s2 = DStream.ofType(String.class, "bar");
		
		Future<Stream<Stream<Tuple2<String, String>>>> resultFuture = s1.join(s2).on(a -> true).partition().executeAs(this.applicationName);
		Stream<Stream<Tuple2<String, String>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Tuple2<String, String>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(2, resultStreams.size());
		result.close();
		
	}
}
