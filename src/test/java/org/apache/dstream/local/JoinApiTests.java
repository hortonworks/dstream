package org.apache.dstream.local;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateResult;
import org.apache.dstream.OutputSpecification;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.DistributableSource;
import org.apache.dstream.Submittable;
import org.apache.dstream.io.TextSource;

public class JoinApiTests {

	public void join() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		
		DistributableSource<String> source = TextSource.create(path).forJob("foo");
		
		IntermediateResult<String, Integer> resultA = source.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		IntermediateResult<String, Integer> resultB = source.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		resultA.join(resultB);
	}
	
	public void joinWithDifferentValueTypes() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		DistributableSource<String> source = TextSource.create(path).forJob("foo");
		
		IntermediateResult<String, Integer> resultA = source.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		IntermediateResult<String, Long> resultB = source.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1L, Long::sum))
	    );
		
		Submittable<Entry<String, String>> submittable = resultA.join(resultB, (a, b) -> "six").partition(4);
	}
}
