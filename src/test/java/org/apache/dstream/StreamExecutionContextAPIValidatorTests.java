package org.apache.dstream;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.dstream.utils.Utils.*;

import org.apache.dstream.io.CollectionStreamableSource;
import org.apache.dstream.io.OutputSpecification;
import org.apache.dstream.io.StreamableSource;
import org.apache.dstream.io.TextSource;
import org.apache.dstream.utils.Partitioner;
import org.apache.dstream.utils.Utils;
import org.junit.Test;

/**
 * This test simply validates the type-safety and the API, so its successful compilation
 * implies overall success of this test.
 */
@SuppressWarnings("unused")
public class StreamExecutionContextAPIValidatorTests { 
	
	/**
	 * This is the example of quintessential WordCount with a few extras.
	 * See comments in line
	 * 
	 * @throws Exception
	 */
	public void wordCount() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		// Defines an FS based Stream source with resource type (e.g., text), key/value types etc.
		InputStream is = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				// Defines a KeyValue computation stage which allows one to define output KeyValue types and computation logic
				// Computation logic is build using standard Java 8 Stream API and can end in terminal or intermediate function call. 
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					// essentially a combiner function.
					.collect(Collectors.<String, String, Integer>toMap(s -> s, s -> 1, Integer::sum))
				// Upon completion stages write to a shuffle or final output
				// In this case we are asking for 10 reducers while also providing merge function which could be the same as the one used in collector
				).partition(10, Integer::sum)
				.computeKeyValue(Integer.class,  Integer.class, stream -> stream
					.filter(s -> s.getValue() > 7)
					.map(s -> s)
				)
				.saveAs(MockOutputSpec.get()).toInputStream();
	}
	
	
	/**
	 * Will expose raw {@link InputStream} to the result data set
	 */
	public void withResultInputStream() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		InputStream is = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.filter(s -> s.startsWith("foo"))
					.collect(Collectors.<String, String, Integer>toMap(s -> s, s -> 1, Integer::sum))
				).partition(10, Integer::sum)
				.computeKeyValue(Integer.class,  Integer.class, stream -> stream
					.collect(Collectors.<Entry<String, Integer>, Integer, Integer>toMap(s -> 34, s -> 1, Integer::sum))
				).partition(2, Integer::sum)
				.saveAs(MockOutputSpec.get()).toInputStream();
	}
	/**
	 * Will expose {@link Stream} to the result data set allowing result data to be streamed for local processing (e.g., iterate over results)
	 */
	public void withResultStream() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Stream<Entry<String, Integer>> resultStream = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).partition(2, Integer::sum)
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.collect(Collectors.<Entry<String, Integer>, String, Integer>toMap(s -> s.getKey(), s -> s.getValue(), Integer::sum))
				)
				.saveAs(MockOutputSpec.get()).stream();
	}
	
	/**
	 * Same as above, but with multiple stages. This is pure optimization since one can easily create a new 
	 * StreamExecutionContext from the result, thus creating a new distributed stream processing context. However in a case 
	 * of something like Tez, Spark etc., this would result in a new DAG. The multiple stage approach allows 
	 * the result of the first stream to be treated as an intermediate result of a stage within a multi-stage single DAG
	 * See {@link #multiDag()}
	 */
	public void multiStage() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).partition(4, Integer::sum)
				.computeKeyValue(Integer.class, Integer.class, stream -> stream
					.filter(s -> s.getValue() == 4)
					.collect(Collectors.<Entry<String, Integer>, Integer, Integer>toMap(s -> 5, s -> s.getValue(), Integer::sum))
				).partition(2, Integer::sum)
				.saveAs(MockOutputSpec.get()).stream();
	}
	
	/**
	 * Partitioning for cases where no additional reduction needs to happen
	 */
	public void partitioning() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Stream<Entry<String, Integer>> streamable = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1))
				).partition(3, Integer::sum)
				.saveAs(MockOutputSpec.get()).stream();
	}
	
	/**
	 * Partitioning for cases where no additional reduction needs to happen
	 */
	public void partitioningWithLamda() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Stream<Entry<String, Integer>> streamable = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1))
				).partition(s -> s.getKey().hashCode(), Integer::sum)
				.saveAs(MockOutputSpec.get()).stream();
	}
	
	/**
	 * Same as above but each stage is represented as a separate DAG.
	 */
	public void multiDag() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		StreamableSource<Entry<String, Integer>> streamable = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(s -> s.toUpperCase())
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				).partition(2, Integer::sum)
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.filter(s -> s.getValue() == 4)
					.collect(Collectors.<Entry<String, Integer>, String, Integer>toMap(s -> s.getKey(), s -> s.getValue(), Integer::sum))
				)
				.saveAs(MockOutputSpec.get()).getSource();
		
		StreamExecutionContext.of(streamable)
				.computeKeyValue(Integer.class, Integer.class, stream -> stream
					.filter(s -> false)
					.collect(Collectors.toMap(s -> 1, s -> 1, Integer::sum))
				).partition(4, Integer::sum)
				.saveAs(MockOutputSpec.get()).stream();
	}
	
	
	public void withCollectionStreamableSource() throws Exception {
		Stream<Entry<String, Integer>> streamable = StreamExecutionContext.of(CollectionStreamableSource.<String>create(Arrays.asList(new String[]{"hi", "bye"})))
				.computeKeyValue(String.class, Integer.class, stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.collect(Collectors.toMap(s -> s, s -> 1))
				).partition(s -> s.getKey().hashCode(), Integer::sum)
				.saveAs(MockOutputSpec.get()).stream();
	}
	
	/**
	 * The following 'terminal' tests signify no continuation (hence the word terminal). In other words the compute 
	 * is under a contract to simply return the results of the stream processing as is (e.g., Map, Long, String etc.). 
	 */
	public void computeTerminalMap() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		Map<String, Integer> map = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.compute(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(s -> s.toUpperCase())
					.collect(Collectors.toMap(s -> s, s -> 1, Integer::sum))
				);
	}
	
	/**
	 */
	public void computeTerminalLong() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		long count = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.compute(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.count()
				);
	}
	
	/**
	 */
	public void computeTerminalWithOptional() throws Exception {
		Path path = FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
		String result = StreamExecutionContext.of(TextSource.create(Long.class, String.class, path))
				.compute(stream -> stream
						.flatMap(s -> Stream.of(s.split("\\s+")))
						.reduce((a, b) -> a + b.toUpperCase()).get()
				);
	}
	
	/**
	 */
	public static class MockOutputSpec implements OutputSpecification{
		public static MockOutputSpec get(){
			return new MockOutputSpec();
		}

		@Override
		public Path getOutputPath() {
			try {
				return FileSystems.getFileSystem(new URI("file:///")).getPath("src/test/java/org/apache/dstream/sample.txt");
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		public <T> StreamableSource<T> toStreamableSource() {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public static class MockPartitioner implements Partitioner{
		public static MockPartitioner get(){
			return new MockPartitioner();
		}

		@Override
		public <T> int getPartition(T input, int reduceTasks) {
			return 0;
		}
	}
}
