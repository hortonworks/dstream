package org.apache.dstream.tez;

import static io.dstream.utils.KVUtils.kv;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;

import io.dstream.DStream;
import io.dstream.SerializableStreamAssets.SerBinaryOperator;
import io.dstream.utils.KVUtils;
import junit.framework.Assert;

public class MapSideCombineTests extends BaseTezTests {

	private final String applicationName = this.getClass().getSimpleName();

	private final TestCombiner bo = new TestCombiner(getUnsafe().allocateMemory(4));

	@After
	public void after(){
		clean(applicationName);
		this.bo.reset();
	}

	@Test
	public void computeMapSideCombine() throws Exception {
		DStream<String> sourcePipeline = DStream.ofType(String.class, "ms");

		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				).reduceValues(s -> s.getKey(), s -> s.getValue(), bo)
				.executeAs(this.applicationName);

		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);

		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
		assertEquals(3, this.bo.getTotalInvocations());
	}

	@Test
	public void computeReduceSideCombineOnly() throws Exception {
		DStream<String> sourcePipeline = DStream.ofType(String.class, "rs");

		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				).reduceValues(s -> s.getKey(), s -> s.getValue(), bo)
				.executeAs(this.applicationName);

		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);

		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
		assertEquals(1, this.bo.getTotalInvocations());
	}

	@Test
	public void streamMapSideCombine() throws Exception {
		DStream<String> sourceStream = DStream.ofType(String.class, "ms");

		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				//				.reduceValues(s -> s, s -> 1, bo)
				.map(word -> kv(word, 1))
				.reduceValues(s -> s.getKey(), s -> s.getValue(), bo)
				.executeAs(this.applicationName);

		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(1000000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);

		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
		assertEquals(3, this.bo.getTotalInvocations());
	}

	@Test
	public void streamReduceSideCombineOnly() throws Exception {
		DStream<String> sourceStream = DStream.ofType(String.class, "rs");

		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.reduceValues(s -> s.getKey(), s -> s.getValue(), bo)
				.executeAs(this.applicationName);

		Stream<Stream<Entry<String, Integer>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);
		List<Stream<Entry<String, Integer>>> resultStreams = result.collect(Collectors.toList());
		Assert.assertEquals(1, resultStreams.size());
		Stream<Entry<String, Integer>> firstResultStream = resultStreams.get(0);

		List<Entry<String, Integer>> firstResult = firstResultStream.collect(Collectors.toList());
		Assert.assertEquals(10, firstResult.size());
		Assert.assertEquals(KVUtils.kv("bar", 3), firstResult.get(0));
		Assert.assertEquals(KVUtils.kv("dee", 4), firstResult.get(4));
		Assert.assertEquals(KVUtils.kv("doo", 5), firstResult.get(7));

		result.close();
		assertEquals(1, this.bo.getTotalInvocations());
	}

	private static class TestCombiner implements SerBinaryOperator<Integer> {
		private static final long serialVersionUID = 8366519776101104961L;
		private final long pointer;

		private boolean invoked;

		public TestCombiner(long pointer) {
			this.pointer = pointer;
			this.reset();
		}

		public int getTotalInvocations(){
			return getUnsafe().getInt(this.pointer);
		}

		public void reset(){
			getUnsafe().putInt(this.pointer, 0);
		}

		@Override
		public Integer apply(Integer t, Integer u) {
			if (!this.invoked){
				int invocations = getUnsafe().getInt(this.pointer)+1;
				getUnsafe().putInt(this.pointer, invocations);
				this.invoked = true;
			}
			return t + u;
		}
	}
}
