### Distributed Streams
==========

Java 8 Streams and lambdas introduced several abstractions that greatly simplify data processing by exposing a set of well-known _collection 
processing patterns_ (e.g., map, group, join etc.) together with _functional programming paradigms_. 

**Distributed Streams** - provides an API, which builds on realization of a clear separation of concerns between _**data processing**_
and _**data distribution**_, allowing [Streams API](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html) (used for _**data processing**_) 
to evolve naturally and in isolation, while unobtrusively adding functionality **only** to address _**data distribution**_ concerns.

The following code snippet depicts a quintessential _WordCount_ and how it is realized using the API provided by **Distributed Streams**:

```java
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DistributedPipeline;
import org.apache.dstream.io.TextSource;

public class WordCount {

	public static void main(String... args) throws Exception {
		FileSystem fs = FileSystems.getFileSystem(new URI("hdfs:///"));
		Path inputPath = fs.getPath("samples.txt");
		
		DistributedPipeline<String> sourcePipeline = TextSource.create(inputPath)
													.asPipeline("WordCount");
		
		Stream<Entry<String, Integer>> result = sourcePipeline.computePairs(stream -> stream
				  .flatMap(s -> Stream.of(s.split("\\s+")))
				  .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)))
		  .aggregate(2, Integer::sum)
		  .save(fs).toStream();
		
		// print results to console
		result.forEach(System.out::println);
	}
}
```