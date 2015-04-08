package demo;

import java.io.File;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.support.UriSourceSupplier;
import org.apache.dstream.utils.Utils;
public class WordCountPipe {
	
	public static void main(String... args) throws Exception {
	
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class,  UriSourceSupplier.from(new File("src/test/java/org/apache/dstream/local/sample.txt").toURI()));
		
		Stream<Entry<String, Integer>>[] result = sourcePipeline.<String, Integer>computeKeyValues(stream -> stream
					.flatMap(line -> Stream.of(line.split(" ")))
					.map(word -> Utils.kv(word,  1))
		).reduceByKey(Integer::sum)
		 .executeAs("WordCount");
		
//		result.forEach(System.out::println);
		
//		sourcePipeline
//			.<Entry<String, Integer>>compute(stream -> stream
//					.flatMap(line -> Stream.of(line.split(" ")))
////					.map(word -> Utils.kv(word,  1))
////					.map(s -> "")
//					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
//		   ).reduce(s -> s.getKey(), s -> Long.valueOf(s.getValue()), Long::sum)
//		    .<String>compute(stream -> stream
//		    		.filter(s -> false)
//		    		.map(s -> "Hello")
//		   ).partition()
//		    .<Entry<String, Integer>>compute(stream -> stream
//				   .filter(s -> false)
//				   .map(s -> Utils.kv(s,  1))
//		   ).group(s -> s.getKey());
		    
		    /*
		     * Grouping, Combining, Joining - all depend on KV
		     */
		 
		// collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
		// we could probably provide our own collector factory to have toMap method that returns Stream to avoid entrySet().stream()

	}
}
