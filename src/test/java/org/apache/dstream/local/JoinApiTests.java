package org.apache.dstream.local;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.IntermediateResult;
import org.apache.dstream.StreamExecutionContext;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.TextSource;

public class JoinApiTests {

	public void join() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext<String> ec = StreamExecutionContext.of("foo", TextSource.create(path));
		
		IntermediateResult<String, Integer> resultA = ec.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		IntermediateResult<String, Integer> resultB = ec.computePairs(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		resultA.join(resultB);
	}
}
