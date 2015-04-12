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

The following code snippets shows two styles of API provided by this project and both depict a quintessential _WordCount_:

_**DistributableStream**_
```java
SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, sourceSupplier);
		
Stream<Stream<?>> result = sourceStream
			.flatMap(line -> Stream.of(line.split("\\s+")))
			.reduce(word -> word, word -> 1, Integer::sum)
			.executeAs("WordCount");
		
result.forEach(stream -> stream.forEach(System.out::println));
		
result.close();
```

_**DistributablePipeline**_
```java
SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		
Stream<Stream<Entry<String, Integer>>> result = sourcePipeline.compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
			)
			.reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			.executeAs("WordCount");
		
result.forEach(stream -> stream.forEach(System.out::println));
		
result.close();
```

Producing output similar to this:
```
We=4
cannot=2
created=3
our=1
problems=2
same=1
solve=1
the=4
...
```

For more information and details please follow [documentation](https://github.com/hortonworks/dstream/wiki)

=======

