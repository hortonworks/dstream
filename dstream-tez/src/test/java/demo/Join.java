package demo;

import java.io.File;

import org.apache.dstream.tez.TezConstants;

/**
 * 
 */
public class Join {

//	public static void main(String... args) throws Exception {
//		File file1 = new File("src/test/java/demo/file1.txt");
//		prepare(file1);
//		File file2 = new File("src/test/java/demo/file2.txt");
//		prepare(file2);
//		
//		FileSystem fs = FileSystems.getFileSystem(new URI("hdfs:///"));
//		Path in1 = fs.getPath(file1.getName());
//		Path in2 = fs.getPath(file2.getName());
//		
//		DataPipeline<String> p2 = TextSource.create(in2).asPipeline("Join");
//		
//		DataPipeline<String> p1 = TextSource.create(in2).asPipeline("Join");
//		
//		Distributable<Integer, String> d2 =  p2.<Integer, String>computeMappings(stream -> stream
//					.map(s -> s.split("\\s+"))
//					.collect(Collectors.toMap(s -> parseInt(s[0]), s -> s[1]))
//				);
//		
//		p1.<Integer, Pair<String, String>>computeMappings(stream -> stream
//				.map(s -> s.split("\\s+"))
//				.collect(Collectors.toMap(s -> parseInt(s[2]), s -> new Pair(s[0], s[1])))
//			)
//		.join(d2)
//		.partition(5)
//		.save(fs);
//		  
//		  
//		
//	}
	
	
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
