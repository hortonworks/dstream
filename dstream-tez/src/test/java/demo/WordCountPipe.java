package demo;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.tez.BaseTezTests;
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
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = DistributablePipeline.ofType(String.class, "wc").compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
			).combine(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			.executeAs("WordCount");
		
		AtomicInteger i = new AtomicInteger();
		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		result.forEach(stream -> {
			System.out.println("Partition: " + i.getAndIncrement());
			stream.forEach(System.out::println);
			});
		
		result.close();
		BaseTezTests.clean("WordCount");
		System.exit(0);
	}
	
	
	/**
	 * Will copy sample file to HDFS
	 */
	private static URI prepare(File localFile) throws Exception {
		System.setProperty(TezConstants.GENERATE_JAR, "true");
		org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration());
		org.apache.hadoop.fs.Path in = new org.apache.hadoop.fs.Path(localFile.getAbsolutePath());
		org.apache.hadoop.fs.Path out = new org.apache.hadoop.fs.Path(localFile.getName());
		
		if (!hdfs.exists(out)){
			hdfs.copyFromLocalFile(in, out);
			if (hdfs.exists(out)){
				System.out.println("Successfully copied " + in + " to HDFS: " + out);
			}
		} 
		out = out.makeQualified(hdfs.getUri(), hdfs.getWorkingDirectory());
		
		System.out.println("HDFS URI: " + out.toUri());
		return out.toUri();
	}
}
