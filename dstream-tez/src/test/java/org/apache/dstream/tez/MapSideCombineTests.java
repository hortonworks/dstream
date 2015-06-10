package org.apache.dstream.tez;

import static org.apache.dstream.utils.KVUtils.kv;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junit.framework.Assert;

import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.DistributableStream;
import org.apache.dstream.support.SerializableFunctionConverters.BinaryOperator;
import org.apache.dstream.utils.JvmUtils;
import org.apache.dstream.utils.KVUtils;
import org.junit.After;
import org.junit.Test;

public class MapSideCombineTests extends BaseTezTests {
	
	private final String applicationName = this.getClass().getSimpleName();
	
	private final TestCombiner bo = new TestCombiner(JvmUtils.getUnsafe().allocateMemory(4));
	
	@After
	public void after(){
		clean(applicationName);
		this.bo.reset();
	}

	@Test
	public void pipelineMapSideCombine() throws Exception {	
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			).combine(s -> s.getKey(), s -> s.getValue(), bo)
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
	public void pipelineReduceSideCombineOnly() throws Exception {	
		DistributablePipeline<String> sourcePipeline = DistributablePipeline.ofType(String.class, "rs");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourcePipeline.<Entry<String, Integer>>compute(stream -> stream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
			).combine(s -> s.getKey(), s -> s.getValue(), bo)
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
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "ms");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.combine(s -> s.getKey(), s -> s.getValue(), bo)
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
		assertEquals(3, this.bo.getTotalInvocations());
	}
	
	@Test
	public void streamReduceSideCombineOnly() throws Exception {
		DistributableStream<String> sourceStream = DistributableStream.ofType(String.class, "rs");
		
		Future<Stream<Stream<Entry<String, Integer>>>> resultFuture = sourceStream
				.flatMap(line -> Stream.of(line.split("\\s+")))
				.map(word -> kv(word, 1))
				.combine(s -> s.getKey(), s -> s.getValue(), bo)
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
	
	private static class TestCombiner implements BinaryOperator<Integer> {
		private static final long serialVersionUID = 8366519776101104961L;
		private final long pointer;
		
		private boolean invoked;
		
		public TestCombiner(long pointer) {
			this.pointer = pointer;
			this.reset();
		}
		
		public int getTotalInvocations(){
			return JvmUtils.getUnsafe().getInt(this.pointer);
		}
		
		public void reset(){
			JvmUtils.getUnsafe().putInt(this.pointer, 0);
		}

		@Override
		public Integer apply(Integer t, Integer u) {
			if (!this.invoked){
				int invocations = JvmUtils.getUnsafe().getInt(this.pointer)+1;
				JvmUtils.getUnsafe().putInt(this.pointer, invocations);
				this.invoked = true;
			}
			return t + u;
		}
	}
}
