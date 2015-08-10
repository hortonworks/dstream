package examples;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public abstract class SampleBase {

	public static <T> void printResults(Stream<Stream<T>> resultPartitions){
		AtomicInteger partitionCounter = new AtomicInteger();
		resultPartitions.forEach(resultPartition -> {
			System.out.println("\n=> PARTITION:" + partitionCounter.getAndIncrement());
			resultPartition.forEach(System.out::println);
		});
	}
}
