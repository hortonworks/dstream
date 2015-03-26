package org.apache.dstream.local;

import static org.apache.dstream.utils.Utils.toEntry;

import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.dstream.ItemReader;
import org.apache.dstream.KeyValueProcessor;
import org.apache.dstream.Pipeline;
import org.apache.dstream.ResultWriter;
import org.apache.dstream.ValuesProcessor;

/**
 * This test simply validates the type-safety and the API, so its successful compilation
 * implies overall success of this test.
 */

public class StreamExecutionContextAPIValidatorTests { 
	Pipeline<String> pipeline = null;
	
	public void computeMappingsWithStreamFunction() throws Exception {	
		pipeline.computeMappings(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(word -> toEntry(word, 1))
		  );
	}
	
	public void computeMappingsWithStreamFunctionAndMapSideCombiner() throws Exception {	
		pipeline.computeMappings(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(word -> toEntry(word, 1)), Integer::sum
		  );
	}
	
	public void computeMappingsWithStreamFunctionAndMappingsContinuationInCombine() throws Exception {	
		pipeline.<String, Integer>computeMappings(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(word -> toEntry(word, 1))
		  ).<Long, Boolean>computeMappings(Integer::sum, stream -> stream
				  .map(s -> toEntry(1L, false))
		  );  
	}
	
	public void computeMappingsWithStreamFunctionAndMappingsContinuationOutCombine() throws Exception {	
		pipeline.<String, Integer>computeMappings(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(word -> toEntry(word, 1))
		  ).<Long, Boolean>computeMappings(stream -> stream
				  .map(s -> toEntry(1L, false)), (a,b) -> true
		  );  
	}
	
	public void computeMappingsWithStreamFunctionAndMappingsContinuationInOutCombine() throws Exception {	
		pipeline.<String, Integer>computeMappings(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(word -> toEntry(word, 1))
		  ).<Long, Boolean>computeMappings(Integer::sum, stream -> stream
				  .map(s -> toEntry(1L, false)), (a,b) -> true
		  );  
	}
	
	public void computeMappingsWithProcessor() throws Exception {	
		pipeline.<String, Integer> computeMappings(new KeyValueProcessor<String, String, Integer>() {
			@Override
			public Void process(ItemReader<String> reader,
					ResultWriter<Entry<String, Integer>> writer) {
//				String[] words = reader.read().split("\\s+");
				for (String word : reader.read().split("\\s+")) {
					writer.write(toEntry(word, 1));
				}
				return null;
			}
		});
	}
	
	public void computeValuesWithStreamFunction() throws Exception {	
		/*
		 * While it may be returning the same Entry as in previous test, the semantics of the 
		 * operator imply that such value will be treated as single value (e.g., serialized as a whole, not as Key/Value pair)
		 */
		pipeline.computeValues(stream -> stream
					.flatMap(s -> Stream.of(s.split("\\s+")))
					.map(word -> toEntry(word, 1))
		  );
	}
	
	public void computeValuesWithValuesProcessor() throws Exception {	
		/*
		 * While it may be returning the same Entry as in previous test, the semantics of the 
		 * operator imply that such value will be treated as single value (e.g., serialized as a whole, not as Key/Value pair)
		 */
		pipeline.<Entry<String, Integer>>computeValues(new ValuesProcessor<String, Entry<String, Integer>>() {
			@Override
			public Void process(ItemReader<String> reader,
					ResultWriter<Entry<String, Integer>> writer) {
				return null;
			}
		});
	}
}
