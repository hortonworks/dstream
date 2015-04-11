package org.apache.dstream.local;


public class GroupByApiTests {

//	public void join() throws Exception {
//		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
//		Pipeline<String> source = TextSource.create(path).asPipeline("foo");
//		
//		Distributable<String, Integer> resultA = source.computeMappings(stream -> stream
//				.flatMap(s -> Stream.of(s.split("\\s+")))
//				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
//	    );
//		
//		resultA.groupByKey();
//	}
}
