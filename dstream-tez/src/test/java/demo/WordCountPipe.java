package demo;

import java.io.File;
import java.net.URI;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
		File file = new File("src/test/java/demo/monte-cristo.txt");
//		URI source = prepare(file);
		System.setProperty(TezConstants.UPDATE_CLASSPATH, "false");
		System.setProperty(TezConstants.GENERATE_JAR, "true");
		
		URI source = new URI("hdfs://cn041-10.l42scl.hortonworks.com:8020/user/hive/external/tpch-1000/lineitem");
		
		SourceSupplier<URI> sourceSupplier = UriSourceSupplier.from(source);
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, sourceSupplier);
		Stream<Stream<Entry<String, Integer>>> result = sourcePipeline.compute(stream -> stream
				.flatMap(line -> Stream.of(line.split(",")))
				.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum)).entrySet().stream()
			)
			.reduce(s -> s.getKey(), s -> s.getValue(), Integer::sum)
			.executeAs("WordCount");
		
		AtomicInteger i = new AtomicInteger();
		
		result.forEach(stream -> {
			System.out.println("Partition: " + i.getAndIncrement());
			stream.limit(10).forEach(System.out::println);
			});
		
		result.close();
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
