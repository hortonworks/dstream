package io.dstream.tez;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Test;

import io.dstream.DStream;
import io.dstream.support.AbstractPartitionedStreamProducingSourceSupplier;
import io.dstream.utils.ExecutionResultUtils;

public class NonURISourcesExecutionTests extends BaseTezTests {


	@Test
	public void test() throws Exception {
		String executionName = "NonURISourcesExecutionTests";
		clean(executionName);
		DStream<String> ds = DStream.ofType(String.class, "wc");
		Future<Stream<Stream<String>>> resultFuture = ds.executeAs(executionName);
		Stream<Stream<String>> resultPartitionedStream = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		ExecutionResultUtils.printResults(resultPartitionedStream, true);
		clean(executionName);
	}

	/**
	 *
	 */
	public static class SampleCollectionSupplier extends AbstractPartitionedStreamProducingSourceSupplier<String> {
		private static final long serialVersionUID = -7041001912896486135L;

		public SampleCollectionSupplier(Properties executionConfig, String executionGraphName) {
			super(executionConfig, executionGraphName);
		}
		@Override
		protected Stream<String> doGet(int partitionId) {
			return Stream.of("foo bar baz".split("\\s+"));
		}
	}
}
