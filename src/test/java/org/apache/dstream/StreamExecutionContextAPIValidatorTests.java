package org.apache.dstream;

import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.io.ListStreamableSource;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.io.TextSource;
import org.apache.dstream.utils.Partitioner;
import org.junit.Test;

import sun.reflect.generics.reflectiveObjects.TypeVariableImpl;

/**
 * This test simply validates the type-safety and the API, so its successful compilation
 * implies overall success of this test.
 */
@SuppressWarnings("unused")
public class StreamExecutionContextAPIValidatorTests { 
	
	public void computePairs() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of(TextSource.create(path));
		
		ec.computePairs(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
		  ).partition(3, Integer::sum)
		   .saveAs(outputSpec);
	}
	
	public void computePairsWithContinuation() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of(TextSource.create(path));
		
		ec.<String, Integer>computePairs(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
		  ).partition(3, Integer::sum)
		   .computePairs(stream -> stream
				   	 .filter(s -> true)
				   	 .collect(Collectors.groupingBy(s -> s))
		  ).partition(1, (a, b) -> a);
	}
	
	public void computeBoolean() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		boolean result = StreamExecutionContext.of(TextSource.create(path))
				.computeBoolean(stream -> !stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
					.isEmpty()
				);
	}
	
	public void computeInt() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		int result = StreamExecutionContext.of(TextSource.create(path))
				.computeInt(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
					.size()
				);
	}
	
	public void computeLong() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		long result = StreamExecutionContext.of(TextSource.create(path))
				.computeLong(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.count()
				);
	}
	
}
