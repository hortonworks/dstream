package org.apache.dstream;

import java.io.InputStream;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.dstream.utils.Utils.*;

import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.TextFile;

@SuppressWarnings("unused")
public class StreamExecutionContextTypeCompileTests {

	/**
	 * Will expose raw {@link InputStream} to the result data set
	 */
	
	public void withResultInputStream(){
		InputStream is = StreamExecutionContext.of(TextFile.create(Long.class, String.class, "hdfs://hdp.com/foo/bar/hey.txt"))
				.computeAsKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.filter(s -> s.startsWith("foo"))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).reduceByKey((a,b) -> a + b, 2)
				.saveAs(MockOutputSpec.get()).toInputStream();
	}
	/**
	 * Will expose {@link Stream} to the result data set allowing result data to be streamed for local processing (e.g., iterate over results)
	 */
	public void withResultStream(){
		Stream<Entry<String, Integer>> resultStream = StreamExecutionContext.of(TextFile.create(Long.class, String.class, "hdfs://hdp.com/foo/bar/hey.txt"))
				.computeAsKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).reduceByKey((a,b) -> a + b, 2)
				.saveAs(MockOutputSpec.get()).toStream();
	}
	
	/**
	 * Same as above, but with multiple stages. This is pure optimization since one can easily create a new 
	 * StreamExecutionContext from the result, thus creating a new distributed stream processing context. However in a case 
	 * of something like Tez, Spark etc., this would result in a new DAG. The multiple stage approach allows 
	 * the result of the first stream to be treated as an intermediate result of a stage within a multi-stage single DAG
	 * See {@link #multiDag()}
	 */
	public void multiStage(){
		StreamExecutionContext.of(TextFile.create(Long.class, String.class, "hdfs://hdp.com/foo/bar/hey.txt"))
				.computeAsKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).reduceByKey((a,b) -> a + b, 2)
				.computeAsKeyValue(Integer.class, Integer.class, stream -> stream
					.filter(s -> false)
					.collect(Collectors.toMap(s -> 1, s -> 1, Integer::sum))
				).reduce((a,b) -> toEntry(a.getValue(), a.getValue()), 4)
				.saveAs(MockOutputSpec.get()).toStream();
	}
	
	/**
	 * Same as above but each stage is represented as a separate DAG.
	 */
	public void multiDag(){
		Streamable<Entry<String, Integer>> streamable = StreamExecutionContext.of(TextFile.create(Long.class, String.class, "hdfs://hdp.com/foo/bar/hey.txt"))
				.computeAsKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(s -> s.toUpperCase())
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).reduceByKey((a,b) -> a + b, 2)
				.saveAs(MockOutputSpec.get()).getSource();
		
		StreamExecutionContext.of(streamable)
				.computeAsKeyValue(Integer.class, Integer.class, stream -> stream
					.filter(s -> false)
					.collect(Collectors.toMap(s -> 1, s -> 1, Integer::sum))
				).reduce((a,b) -> toEntry(a.getValue(), a.getValue()), 4)
				.saveAs(MockOutputSpec.get()).toStream();
	}
	
	/**
	 */
	public static class MockOutputSpec implements OutputSpecification{
		public static MockOutputSpec get(){
			return new MockOutputSpec();
		}
	}
}
