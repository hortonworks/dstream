### DStream - Distributable Streams
==========
> IMPORTANT: At the moment this is a research project with the primary goal of investigating the feasability of the approach.

_The primary focus of the **DStream API** is to provide a [Stream-based](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html) unified programming model to build **ETL-style** processes for **distributable data** and execute them in compatible target systems. While agnostic to any specific type of target system, the API exposes an extensible integration/delegation model to support a system of choice._

The key distinction between [Java 8 Stream](http://docs.oracle.com/javase/8/docs/api/java/util/stream/package-summary.html) and _DStream_ is the notion of _**distributable data**_, which implies that the actual data _may or may not_ be distributed, making _DStream_ somewhat of a universal strategy to build _**ETL-style**_ processes regardless of the location and/or the type of data as well as the execution system.

The following code snippet shows an example of a quintessential _WordCount_:

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

For features overviewo please follow [**Core Features Overview**](https://github.com/hortonworks/dstream/wiki/Core-Features-Overview)

To get started please follow [**Getting Started**](https://github.com/hortonworks/dstream/wiki)

=======

