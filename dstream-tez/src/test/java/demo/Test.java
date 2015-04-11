package demo;

import java.util.Map.Entry;
import java.util.function.Function;

import org.apache.dstream.utils.Utils;

public class Test {

	public static void main(String[] args) {
		Function<Integer, String> f1 = Test::create;
		//Function<String, Entry<?,?>> f2 = Test::split;
		
		Function F1 = f1;
		//Function F2 = f2;
		
		Function F3 = F1.andThen(new MyFunction(s -> s, s -> ((String)s).toUpperCase() + 1 ));
		
		System.out.println(F3.apply(50));
	}
	
	
	public static String create(Integer a){
		return a + "," + (a*2);
	}
	
	public static Entry<Object, Object> toEntry(Function a, Function b){
		return null;
	}

	public static class MyFunction implements Function {
		private final Function key;
		private final Function val;
		public MyFunction(Function key, Function val) {
			this.key = key;
			this.val = val;
		}
		@Override
		public Object apply(Object t) {
			Entry e = Utils.kv(key.apply(t), val.apply(t));
			return e;
		}
		
	}
}
