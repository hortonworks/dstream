package demo;

import java.io.File;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.Source;
import org.apache.dstream.io.TextSource;

public class WordCount {
	public static void main(String... args) throws Exception {
		File localFile = new File("src/test/java/demo/monte-cristo-small.txt");
		
		FileSystem fs = FileSystems.getFileSystem(new URI("hdfs:///"));
		Path inputPath = fs.getPath(localFile.getName());
		
		Source<String> source = TextSource.create(inputPath);
		
		Stream<Entry<String, Integer>> result = source.forJob("WordCount").<Integer>computePairs(stream -> stream
				  .flatMap(s -> Stream.of(s.split("\\s+")))
				  .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)))
		  .aggregate(2, Integer::sum)
		  .save().toStream();
		
		// print results to console
		result.forEach(System.out::println);
	}
}
