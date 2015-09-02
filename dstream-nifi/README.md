### DStream Integration with Apache NiFi.
==========

This project provides all necessary components to realize DStream applications within the [Apache NiFi](https://github.com/apache/nifi). 
![](https://github.com/olegz/general-resources/blob/master/DStream-sample-nifi-flow.png)

### Integration

At the moment the integration model of DStream and Apache NiFi is based on implementing a custom NiFi _Processors_. However, as project evolves new integration points will be added as required.

While implementing NiFi Processor from scrtatch is failrly simple, this project provides an abstract implementation of the NiFi Processor specific to DStream - [AbstractDStreamProcessor](https://github.com/hortonworks/dstream/blob/master/dstream-nifi/src/main/java/org/apache/nifi/dstream/AbstractDStreamProcessor.java). It handles all common functionality leaving sub-classes with a simple task of providing an implementation of a single operation ```getDStream(executionName)```. 
```java
private <T> DStream<T> getDStream(String executionName) {
		return (DStream<T>) DStream.ofType(String.class, "wc")
				.flatMap(record -> Stream.of(record.split("\\s+")))
				.reduceValues(word -> word, word -> 1, Integer::sum);
}
```
For more details and examples please refer to a [Sample Template Project](https://github.com/hortonworks/dstream/tree/master/dstream-dev-template) which aside from examples provides you with the deployment instructions and utilities. 

======

For features overview and Getting started with _**DStream**_ project please follow [**Core Features Overview**](https://github.com/hortonworks/dstream/wiki/Core-Features-Overview) and [**Getting Started**](https://github.com/hortonworks/dstream/wiki) respectively.


=======
