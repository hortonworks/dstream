package demo;

import java.io.File;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.support.UriSourceSupplier;
public class WordCountStream {
	
	public static void main(String... args) throws Exception {
		
		
		DistributableStream<String> stream = DistributableStream.ofType(String.class, UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/local/sample.txt").toURI()));
		Stream[] result = stream.flatMap(line -> Stream.of(line.split("\\s+")))
		      .reduce(word -> word, word -> 1, Integer::sum)
		      .executeAs("WordCount");
		
//		result.forEach(System.out::println);
	}
}
