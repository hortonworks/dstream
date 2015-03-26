package demo;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.Pipeline;
import org.apache.dstream.PipelineFactory;
import org.apache.dstream.utils.Utils;

public class WordCount {
	public static void main(String... args) throws Exception {
		URI uri = new File("data/monte-cristo.txt").toURI();
		
		Pipeline<String> sourcePipeline = PipelineFactory.<String>from(uri);
		
		Stream<Entry<String, Integer>> result = 
			sourcePipeline.<String, Integer>computeMappings(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> Utils.toEntry(word, 1))
			).computeMappings(stream -> stream
				.filter(s -> s.getKey().startsWith("s"))
			).combine(Integer::sum)
			 .submit("WordCount");
		
//		Stream<Entry<String, Integer>> result = sourcePipeline.<String, Integer>computeMappings(stream -> stream
//					.flatMap(line -> Stream.of(line.split("\\s+")))
//					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
//				).submit("WordCount");
		
		result.limit(100).forEach(System.out::println);

	}

}
