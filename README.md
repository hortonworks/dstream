### Distributed Streams
==========

_Java 8 Streams_ and _lambdas_ introduced several abstractions that greatly simplify data processing by exposing a set of well-known _collection 
processing patterns_ (e.g., map, group, join etc.) together with _functional programming paradigms_. 

**Distributed Streams** - provides an API, which builds on _Java 8 Streams_ and _lambdas_ while also maintaining a clear separation of concerns between _**data processing**_
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
As you can see this is a complete sample which includes _import_ statements to demonstrate one of the features of the API, which completely decouples you from the _target execution environment_ 
(e.g., Tez, Spark, Flink etc.) and its dependencies. In fact, this example was copied from [Apache Tez implementation](https://github.com/hortonworks/dstream-tez/) of the Distributed Streams. 
Yet you don't see any dependencies on Tez, Hadoop, HDFS etc. That is because the environment is loaded using standard Java mechanisms for loading services and providers allowing bootstrapping of different execution providers without requiring any changes to the end user code. Custom _FileSystemProviders_ are also supported (HDFS in this case) allowing various file systems to be represented as 
_java.nio.file.FileSystem_, thus keeping your code clean and concise giving you out most flexibility between designing data processing applications vs. choosing the target execution environment for them.  

For more information please read [Getting Started](Getting_Started)

