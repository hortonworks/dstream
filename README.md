### Distributed Streams
==========

_Java 8 Streams_ and _lambdas_ introduced several abstractions that greatly simplify data processing by exposing a set of well-known _collection 
processing patterns_ (e.g., map, group, join etc.) together with _functional programming paradigms_. 

**Distributed Streams** - provides an API, which builds on _Java 8 Streams_ and _lambdas_ while also maintaining a clear separation of concerns between _**data processing**_
and _**data distribution**_, allowing data processing applications to benefit from the rich capabilities of the already available 
[Streams API](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html) (in-memory processing, parallelization etc.), while 
unobtrusively adding functionality **only** to address _**data distribution**_ concerns. While this approach greatly simplifies design, 
development and testing of data processing applications, it also allows [Streams API](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html)
to evolve naturally and in isolation.

The following code snippet depicts a quintessential _WordCount_ and how it is realized using the API provided by **Distributed Streams**:

```java
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.net.URI;

import org.apache.dstream.DistributedPipeline;
import org.apache.dstream.io.TextSource;

public class WordCount {

	public static void main(String... args) throws Exception {
		FileSystem fs = FileSystems.getFileSystem(new URI("hdfs:///"));
		
		Source<String> source = TextSource.create(fs.getPath("samples.txt"));
		
		Stream<Entry<String, Integer>> result = source.asPipeline("WordCount")
			.computePairs(stream -> stream
				  .flatMap(s -> Stream.of(s.split("\\s+")))
				  .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
			)
		    .aggregate(2, Integer::sum)
		    .save(fs).toStream();
		
		// print results to console
		result.forEach(System.out::println);
	}
}
```
This is a complete sample that includes _import_ statements to demonstrate one of the core features of the API, which completely decouples your code from the _target execution environment_ 
(e.g., Tez, Spark, Flink etc.) and its dependencies. In fact, this example was copied from [Apache Tez implementation](https://github.com/hortonworks/dstream-tez/) of the Distributed Streams. 
Yet you don't see any dependencies on Tez, Hadoop, HDFS etc. That is because the target execution environment is completely externalized and is loaded using standard Java mechanisms for configuring and loading _services_ and _providers_, allowing different _target execution providers_ to be loaded without requiring any changes to the end user code. Custom _FileSystemProviders_ are also supported 
(HDFS in this case) allowing for a variety of _java.nio.file.FileSystem_ bindings, thus keeping your code clean and concise giving you out most flexibility between _**designing data processing applications**_ vs. _**choosing their target execution environment**_.  

For more information and details please follow [documentation](https://github.com/hortonworks/dstream/wiki)

=======

