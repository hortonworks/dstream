package org.apache.dstream.local;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DataPipeline;
import org.apache.dstream.Distributable;
import org.apache.dstream.OutputSpecification;
import org.apache.dstream.Triggerable;
import org.apache.dstream.io.TextSource;

public class JoinApiTests {

	public void join() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		
		DataPipeline<String> source = TextSource.create(path).asPipeline("foo");
		
		Distributable<String, Integer> resultA = source.computeMappings(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		Distributable<String, Integer> resultB = source.computeMappings(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		resultA.join(resultB);
	}
	
	public void joinWithDifferentValueTypes() throws Exception {
		OutputSpecification outputSpec = null;
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		DataPipeline<String> source = TextSource.create(path).asPipeline("foo");
		
		Distributable<String, Integer> resultA = source.computeMappings(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		Distributable<String, Long> resultB = source.computeMappings(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1L, Long::sum))
	    );
		Triggerable<Entry<String, String>> submittable = resultA.join(resultB, (a, b) -> "blah").partition(4);
	}
}
