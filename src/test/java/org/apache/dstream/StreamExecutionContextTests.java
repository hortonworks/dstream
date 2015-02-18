package org.apache.dstream;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.dstream.io.TextSource;
import org.apache.dstream.local.StreamExecutionContextImpl;
import org.apache.logging.log4j.util.Strings;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class StreamExecutionContextTests {
	
	
//	public static Map<Integer, Integer> intProcessFunction(Stream<Integer> stream) {
//		return stream
//				.filter(s -> s != 4)
//				.collect(Collectors.<Integer, Integer, Integer>toMap(s -> s, s -> 1, Integer::sum));
//	}
	
	@Test
	public void validateNullSourceException() throws Exception {
		try {
			StreamExecutionContext.of(null);
			Assert.fail();
		} catch (NullPointerException e) {
			Assert.assertTrue(Strings.isNotEmpty(e.getMessage()));
		}
	}
	
	@Test
	public void validateExecutionContextFound() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Object executionContext = StreamExecutionContext.of(TextSource.create(path));
		Assert.assertNotNull(executionContext);
		Assert.assertTrue(executionContext instanceof StreamExecutionContextImpl);
	}
	
//	@Test
//	public void validateFlowWithCollection() throws Exception {
//		Path outputPath = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/out");
//		List<Integer> intList = Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3});
//		StreamExecutionContext.of(ListStreamableSource.<Integer>create(intList, 5))
//				.computeKeyValue(Integer.class, Integer.class, stream -> stream
//						.filter(s -> s != 4)
//						.collect(Collectors.<Integer, Integer, Integer>toMap(s -> s, s -> 1, Integer::sum)))
//				.partition(s -> s.getKey(), Integer::sum)
//				.saveAs(OutputSpecificationImpl.create(outputPath));
////				.computeKeyValue(Integer.class, Integer.class, stream -> stream
////						.filter(s -> s.getKey() == 4)
////						.collect(Collectors.<Entry<Integer, Integer>, Integer, Integer>toMap(s -> s.getKey(), s -> s.getValue(), Integer::sum)))
////				.saveAs(OutputSpecificationImpl.create(outputPath));
//				
//	}
	
	
	
}
