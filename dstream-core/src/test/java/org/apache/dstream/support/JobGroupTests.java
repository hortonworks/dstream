package org.apache.dstream.support;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.DistributableStream;
import org.apache.dstream.JobGroup;
import org.apache.dstream.utils.Pair;

import static org.apache.dstream.utils.KVUtils.kv;

import org.junit.Test;

public class JobGroupTests {

	@Test
	public void testJobGroup(){
		DistributableStream<Entry<String, Integer>> ds1 = DistributableStream.ofType(String.class, "ds1")
				.flatMap(line -> Stream.of(line.split(" ")))
				.filter(word -> word.length() == 4)
				.reduce(word -> word, word -> 1, Integer::sum);
		
		DistributableStream<String> ds2 = DistributableStream.ofType(String.class, "ds2")
				.filter(line -> line.length() <= 20);
		
		DistributableStream<Entry<String, Pair<Integer, Integer>>> ds3 = DistributableStream.ofType(String.class, "ds3")
				.flatMap(line -> Stream.of(line.split(" ")))
				.map(word -> kv(word, 1))
				.join(ds1, hash -> hash.getKey(), hash -> hash.getValue(), probe -> probe.getKey(), probe -> probe.getValue());
		
		JobGroup jg = JobGroup.create("MyJobGroup", ds1, ds2, ds3);
		
		jg.executeAs("MyJobGroup");

	}
}
