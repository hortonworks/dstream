package examples;

import java.util.stream.Stream;

import org.apache.dstream.DStream;
import org.apache.dstream.DStream.DStream2;

public class UnionExamples {

	public static void main(String[] args) throws Exception {
//		BaseTezTests.clean("Join");
//		
		DStream<String> hash = DStream.ofType(String.class, "hash");
		DStream<String> probe = DStream.ofType(String.class, "probe");
		
		DStream2<String, String> joined = hash.join(probe);
		
//		joined.union(joined).map(s -> 1).u;
		
		
		hash.union(probe);
		
//		DStream<Entry<String, Integer>> foo = DStream.ofType(String.class, "probe").map(s -> KVUtils.kv(s, 1));
//		
//		hash.union(probe).union(foo);
		
		Stream<String> a = Stream.of("foo", "bar", "baz");
		Stream<String> b = Stream.of("foo", "baz", "Seva");
		Stream<String> c = Stream.of("baz", "oleg", "Seva");
		
		// ALL
		
//		Stream<String> uAll = Stream.of(a,b,c).reduce(Stream::concat).get();
//		uAll.forEach(System.out::println);
//		
//		System.out.println("===");
		
		Stream<String> u = Stream.of(a,b,c).reduce(Stream::concat).get().distinct();
		u.forEach(System.out::println);
	}

}
