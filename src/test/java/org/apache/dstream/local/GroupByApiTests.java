package org.apache.dstream.local;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.Distributable;
import org.apache.dstream.DistributedPipeline;
import org.apache.dstream.io.TextSource;

public class GroupByApiTests {

	public void join() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		DistributedPipeline<String> source = TextSource.create(path).asPipeline("foo");
		
		Distributable<String, Integer> resultA = source.computeMappings(stream -> stream
				.flatMap(s -> Stream.of(s.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
	    );
		
		resultA.groupByKey();
	}
}
