package org.apache.dstream.local;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.TextSource;

/**
 * This test simply validates the type-safety and the API, so its successful compilation
 * implies overall success of this test.
 */
@SuppressWarnings("unused")
public class StreamExecutionContextAPIValidatorTests { 
	
	public void computePairs() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of("foo", TextSource.create(path));
		
		ec.computePairs(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
		  ).merge(3, Integer::sum)
		   .save(outputSpec);
	}
	
	public void computePairsWithContinuation() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of("foo", TextSource.create(path));
		
		ec.<String, Integer>computePairs(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
		  ).merge(3, Integer::sum)
		   .computePairs(stream -> stream
				   	 .filter(s -> true)
				   	 .collect(Collectors.groupingBy(s -> s))
		  ).merge(1, (a, b) -> a);
	}
	
	public void computeBoolean() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		boolean result = StreamExecutionContext.of("foo", TextSource.create(path))
				.computeBoolean(stream -> !stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
					.isEmpty()
				);
	}
	
	public void computeInt() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		int result = StreamExecutionContext.of("foo", TextSource.create(path))
				.computeInt(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
					.size()
				);
	}
	
	public void computeLong() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		long result = StreamExecutionContext.of("foo", TextSource.create(path))
				.computeLong(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.count()
				);
	}
	
	public void partitionSourceWithFunction() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of("foo", TextSource.create(path));
		
		ec.partition(s -> s.hashCode()).save(outputSpec);
	}
	
	public void partitionSourceWithDefaultPartitioner() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of("foo", TextSource.create(path));
		
		ec.partition(4).save(outputSpec);
	}
	
	public void partitionSourceWithFunctionAfterComputation() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of("foo", TextSource.create(path));
		
		ec.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	  ).partition(s -> s.hashCode())
	   .save(outputSpec);
	}
}
