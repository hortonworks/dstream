package org.apache.dstream.local;


public class JoinApiTests {

//	public void join() throws Exception {
//		OutputSpecification outputSpec = null;
//		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
//		
//		Pipeline<String> source = TextSource.create(path).asPipeline("foo");
//		
//		Distributable<String, Integer> resultA = source.computeMappings(stream -> stream
//				.flatMap(s -> Stream.of(s.split("\\s+")))
//				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
//	    );
//		
//		Distributable<String, Integer> resultB = source.computeMappings(stream -> stream
//				.flatMap(s -> Stream.of(s.split("\\s+")))
//				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
//	    );
//		
//		resultA.join(resultB);
//	}
//	
//	public void joinWithDifferentValueTypes() throws Exception {
//		OutputSpecification outputSpec = null;
//		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
//		Pipeline<String> source = TextSource.create(path).asPipeline("foo");
//		
//		Distributable<String, Integer> resultA = source.computeMappings(stream -> stream
//				.flatMap(s -> Stream.of(s.split("\\s+")))
//				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
//	    );
//		
//		Distributable<String, Long> resultB = source.computeMappings(stream -> stream
//				.flatMap(s -> Stream.of(s.split("\\s+")))
//				.collect(Collectors.toMap(s -> s, s -> 1L, Long::sum))
//	    );
//		Triggerable<Entry<String, String>> submittable = resultA.join(resultB, (a, b) -> "blah").partition(4);
//	}
}
