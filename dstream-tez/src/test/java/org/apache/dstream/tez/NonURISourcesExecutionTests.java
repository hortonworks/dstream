package org.apache.dstream.tez;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Test;

import dstream.DStream;
import dstream.support.AbstractPartitionedStreamProducingSourceSupplier;
import dstream.support.PartitionIdHelper;
import dstream.utils.ExecutionResultUtils;

public class NonURISourcesExecutionTests extends BaseTezTests {


	@Test
	public void test() throws Exception {
		String executionName = "NonURISourcesExecutionTests";
		clean(executionName);
		DStream<String> ds = DStream.ofType(String.class, "wc");
		Future<Stream<Stream<String>>> resultFuture = ds.executeAs(executionName);
		Stream<Stream<String>> resultPartitionedStream = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
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
		public Stream<String> get() {
			int partitionId = PartitionIdHelper.getPartitionId();
			return Stream.of("foo bar baz".split("\\s+"));
		}
	}
}
