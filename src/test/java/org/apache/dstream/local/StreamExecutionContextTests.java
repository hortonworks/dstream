package org.apache.dstream.local;

import java.io.File;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.dstream.Source;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.TextSource;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class StreamExecutionContextTests {
	
	private volatile FileSystem fs;
	
	@Before
	public void before(){
		try {
			this.fs = FileSystems.getFileSystem(new URI("file:///"));
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

//	@Test
//	public void validateNullSourceException() throws Exception {
//		try {
//			StreamExecutionContext.of("foo", null);
//			Assert.fail();
//		} catch (NullPointerException e) {
//			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
//		}
//	}
	
//	@Test
//	public void validateExecutionContextFound() throws Exception {
//		Path path = this.fs.getPath("src/test/java/org/apache/dstream/sample.txt");
//		Object executionContext = StreamExecutionContext.of("foo", TextSource.create(path));
//		Assert.assertNotNull(executionContext);
//		Assert.assertTrue(executionContext instanceof StreamExecutionContextImpl);
//	}
	
	/**
	 * This is the example of quintessential WordCount 
	 */
//	@Test
//	public void fsWordCount() throws Exception {
//		OutputSpecification outputSpec = new OutputSpecificationImpl(this.fs.getPath("src/test/java/org/apache/dstream/out"));
//		
//		StreamSource<String> source = TextSource.create(this.fs.getPath("src/test/java/org/apache/dstream/sample.txt"));
//		
//		
//		
//		source.asJob("").<String, Integer>computePairs(stream -> stream
//					.flatMap(s -> {
//							System.out.println("flatMap: " + s);
//							return Stream.of(s.split("\\s+"));
//						}
//						)
//					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)))
//		  .aggregate(3, Integer::sum)
//		  .save(outputSpec).toStream().forEach(System.out::println);
//	}
	
}
