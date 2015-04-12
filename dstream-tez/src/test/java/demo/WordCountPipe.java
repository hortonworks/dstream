package demo;

import static org.apache.dstream.utils.KVUtils.kv;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.support.SourceSupplier;
import org.apache.dstream.support.UriSourceSupplier;
import org.apache.dstream.tez.TezConstants;
/**
 * 
 */
public class WordCountPipe {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String... args) throws Exception {
		
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(new File("src/test/java/demo/sample.txt").toURI());
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
//		Stream<Entry<String, Integer>>[] result = 
//			sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
//				.flatMap(line -> Stream.of(line.split("\\s+")))
//				.map(word -> kv(word, 1))
//			).reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum).executeAs("WordCount");
		
//		Stream<Entry<Integer, Integer>>[] result = sourcePipeline.reduce(s -> s.hashCode(), s -> 1, Integer::sum).executeAs("WordCount");
			
//		Stream<Entry<String, Integer>>[] result = 
//				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
//					.flatMap(line -> Stream.of(line.split("\\s+")))
//					.map(word -> kv(word, 1))
//				).executeAs("WordCount");
		
//		Stream<Entry<String, Integer>>[] result = 
//				sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
//					.flatMap(line -> Stream.of(line.split("\\s+")))
//					.map(word -> kv(word, 1))
//				).compute(stream -> stream.filter(s -> s.getKey().startsWith("we"))).executeAs("WordCount");
		
		Stream<Entry<String, Integer>>[] result = sourcePipeline
				.reduce(s -> s.hashCode(), s -> 1, Integer::sum)
				.reduce(s -> "VAL", s -> 1, Integer::sum)
				.executeAs("WordCount");
			
//			.reduceByKey(Integer::sum).<String, Integer>computeKeyValues(stream -> stream
//				.filter(s -> s.getValue() == 2)
//				.map(s -> kv(s.getKey().toUpperCase(), s.getValue()))
//			).reduceByKey(Integer::sum)
//			 .executeAs("WordCount");
		
		for (Stream pResult : result) {
			pResult.limit(100).forEach(System.out::println);
		}
		// temporary; implement pipelin.close
		System.exit(0);
	}
	
	
	/**
	 * Will copy sample file to HDFS
	 */
	private static void prepare(File localFile) throws Exception {
		System.setProperty(TezConstants.GENERATE_JAR, "true");
		org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration());
		org.apache.hadoop.fs.Path in = new org.apache.hadoop.fs.Path(localFile.getAbsolutePath());
		org.apache.hadoop.fs.Path out = new org.apache.hadoop.fs.Path(localFile.getName());
		if (!hdfs.exists(out)){
			hdfs.copyFromLocalFile(in, out);
			if (hdfs.exists(out)){
				System.out.println("Successfully copied " + in + " to HDFS");
			}
		} else {
			System.out.println("File " + in + " already exists in HDFS");
		}

	}
}
