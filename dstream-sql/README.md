### DStream Integration with SQL sources
==========

This module provides all necessary components to support integration of DStream with relational data sources

### Integration

Unlike typical DStream processing which defines a language to support sequential data processing patterns, relational data processing already has a language - SQL.
This module's focus is to integrate the _result set_ of the SQL query within DStream API.

```
DStream<Row> sqlDs = SQLDStream.create("sqlDs”); // Convenience factory method. Same as DStream.ofType(Row.class, "sqlDs");
DStream<String> txtDs = DStream.ofType(String.class, "txtDs");

Future<Stream<Stream<Entry<String, List<Row>>>>> resultFuture = sqlDs
	.join(txtDs).on(t2 -> t2._1().get(0).equals(Integer.parseInt(t2._2().split("\\s+")[0])))
	.aggregateValues(t2 -> t2._2().split("\\s+")[1], t2 -> t2._1())
  .executeAs("SQLDStreamTests”);
```
The above example demonstrates the join between the _**relational**_ and _**non-relational**_ data. 

Check out a complete [example](https://github.com/hortonworks/dstream/blob/master/dstream-sql/src/test/java/io/dstream/sql/SQLDStreamTests.java#L38) and its [configuration](https://github.com/hortonworks/dstream/blob/master/dstream-sql/src/test/java/SQLDStreamTests.cfg)

======

For features overview and Getting started with _**DStream**_ project please follow [**Core Features Overview**](https://github.com/hortonworks/dstream/wiki/Core-Features-Overview) and [**Getting Started**](https://github.com/hortonworks/dstream/wiki) respectively.


=======
