### Distributable Streams
==========
> IMPORTANT: At the moment this is a research project with the primary goal of investigating the feasability of the approach.

_Java 8 Streams_ and _lambdas_ introduced several abstractions that greatly simplify data processing by exposing a set of well-known _collection 
processing patterns_ (e.g., map, group, join etc.) together with _functional programming paradigms_. 

**Distributable Streams API** - builds on top of _Java 8 Streams_ and _lambdas_ while attempting to maintain a clear separation of concerns between _**data processing**_
and _**data distribution**_, allowing data processing applications to benefit from the rich capabilities of the already available 
[Streams API](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html) (in-memory processing, parallelization etc.), while 
unobtrusively adding functionality **only** to address _**data distribution**_ concerns. 

While this approach greatly simplifies design, development and testing of the data processing applications that may be dealing with distributed data, it also allows standard [Streams API](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html) to evolve naturally and in isolation. For more details please see the [Vision](https://github.com/hortonworks/dstream/wiki/Vision) document.


The following code snippet shows a quintessential _WordCount_:

_**DistributableStream**_
```java
Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DStream.ofType(String.class, "wc")
    .flatMap(line -> Stream.of(line.split("\\s+")))
    .reduceValues(word -> word, word -> 1, Integer::sum)
  .executeAs("WordCount");

// each stream within a stream represents a partition essentially giving you access 
// to each result partition
Stream<Stream<Entry<String, Integer>>> result = resultFuture.get();
result.forEach(resultPartitionStream -> {
   resultPartitionStream.forEach(System.out::println);
});
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
. . .
```

To get started please follow [Getting Started](https://github.com/hortonworks/dstream/wiki)

=======

