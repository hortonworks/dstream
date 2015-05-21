package org.apache.dstream;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.dstream.support.Aggregators;
import org.apache.dstream.utils.KVUtils;
import org.apache.dstream.utils.Pair;
import org.junit.Test;

public class PredicateJoinFunctionTests {

	@Test
	public void singleValueJoin(){
		KeyValueMappingFunction<Entry<Integer, Company>, Integer, Company> hashKvMapper = 
				new KeyValueMappingFunction<Entry<Integer, Company>, Integer, Company>(h -> h.getKey(), h -> h.getValue());
		
		KeyValueMappingFunction<Entry<Integer, String>, Integer, String> probeKvMapper = 
				new KeyValueMappingFunction<Entry<Integer, String>, Integer, String>(h -> h.getKey(), h -> h.getValue());
		
		
		PredicateJoinFunction<Integer, Company, String> pjf = new PredicateJoinFunction<Integer, Company, String>(hashKvMapper, probeKvMapper);
		
		List<Entry<Integer, Company>> hash = new ArrayList<Entry<Integer,Company>>();
		hash.add(KVUtils.kv(1, new Company("HORTONWORKS")));
		hash.add(KVUtils.kv(2, new Company("CLOUDERA")));
		
		List<Entry<Integer, String>> probe = new ArrayList<Entry<Integer,String>>();
		probe.add(KVUtils.kv(1, "PUBLIC"));
		probe.add(KVUtils.kv(2, "PRIVATE"));
		
		Stream<Stream<? extends Entry<Integer, ? extends Object>>> streamsToJoin = Stream.of(hash.stream(), probe.stream());
		
		List<Entry<Integer, Pair<Company, String>>> result = pjf.apply(streamsToJoin).collect(Collectors.toList());
		
		assertEquals(KVUtils.kv(1, Pair.of(new Company("HORTONWORKS"), "PUBLIC")), result.get(0));
		assertEquals(KVUtils.kv(2, Pair.of(new Company("CLOUDERA"), "PRIVATE")), result.get(1));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void multiProbeValueJoin(){
		KeyValueMappingFunction<Entry<Integer, String>, Integer, String> hashKvMapper = 
				new KeyValueMappingFunction<Entry<Integer, String>, Integer, String>(h -> h.getKey(), h -> h.getValue());
		
		KeyValueMappingFunction<Entry<Integer, Object>, Integer, Object> probeKvMapper = 
				new KeyValueMappingFunction<Entry<Integer, Object>, Integer, Object>(h -> h.getKey(), h -> h.getValue());
		
		
		PredicateJoinFunction<Integer, String, Company> pjf = new PredicateJoinFunction<Integer, String, Company>(hashKvMapper, probeKvMapper);
		
		pjf.composeIntoProbe(new KeyValuesStreamAggregatingFunction(Aggregators::aggregate));
		
		List<Entry<Integer, String>> hash = new ArrayList<Entry<Integer,String>>();
		hash.add(KVUtils.kv(1, "PUBLIC"));
		hash.add(KVUtils.kv(2, "PRIVATE"));
		
		List<Entry<Integer, Iterator<Company>>> probe = new ArrayList<Entry<Integer,Iterator<Company>>>();
		probe.add(KVUtils.kv(1, Arrays.asList(new Company[]{new Company("HORTONWORKS"), new Company("ORACLE"), new Company("IBM")}).iterator()));
		probe.add(KVUtils.kv(2, Arrays.asList(new Company[]{new Company("CLOUDERA"), new Company("MAPR")}).iterator()));
		
		Stream<Stream<? extends Entry<Integer, ? extends Object>>> streamsToJoin = Stream.of(hash.stream(), probe.stream());
		
		List<Entry<Integer, Pair<String, Company>>> result = pjf.apply(streamsToJoin).collect(Collectors.toList());
		assertEquals(KVUtils.kv(1, Pair.of("PUBLIC", Arrays.asList(new Company[]{new Company("HORTONWORKS"), new Company("ORACLE"), new Company("IBM")}))), result.get(0));
		assertEquals(KVUtils.kv(2, Pair.of("PRIVATE", Arrays.asList(new Company[]{new Company("CLOUDERA"), new Company("MAPR")}))), result.get(1));
	}
	
	private static class Company {
		private final String name;
		public Company(String name){
			this.name = name;
		}
		
		@Override
		public String toString(){
			return "CO:" + name;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Company){
				return ((Company)obj).name.equals(this.name);
			}
			else {
				return false;
			}
		}
	}
}
