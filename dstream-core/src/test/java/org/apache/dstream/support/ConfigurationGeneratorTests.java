package org.apache.dstream.support;

import static org.apache.dstream.utils.KVUtils.kv;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.dstream.DistributableConstants;
import org.apache.dstream.DistributablePipeline;
import org.apache.dstream.utils.Pair;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

public class ConfigurationGeneratorTests {

	@Test
	public void testConfigGeneration() throws Exception {
		TestAppender testAppender = new TestAppender();
		Logger.getRootLogger().addAppender(testAppender);
		
		DistributablePipeline<String> hashPipeline = DistributablePipeline.ofType(String.class, "hash");
		DistributablePipeline<String> probePipeline = DistributablePipeline.ofType(String.class, "probe");
		
		DistributablePipeline<String> hash = hashPipeline.compute(stream -> stream
				.map(line -> line.toUpperCase())
		);
		
		DistributablePipeline<Entry<Integer, String>> probe = probePipeline.<Entry<Integer, String>>compute(stream -> stream
				.map(line -> {
					String[] split = line.trim().split("\\s+");
					return kv(Integer.parseInt(split[2]), split[0] + " " + split[1]);
				})
		).reduce(keyVal -> keyVal.getKey(), keyVal -> keyVal.getValue(), (a, b) -> a + ", " + b);
		
		Future<Stream<Stream<Entry<Integer, Pair<String, String>>>>> resultFuture = hash.join(probe, 
				hashElement -> Integer.parseInt(hashElement.substring(0, hashElement.indexOf(" ")).trim()), 
				hashElement -> hashElement.substring(hashElement.indexOf(" ")).trim(), 
				probeElement -> probeElement.getKey(), 
				probeElement -> probeElement.getValue()
			).executeAs(DistributableConstants.GENERATE_CONF + "ConfigurationGeneratorTest");
		
		Stream<Stream<Entry<Integer, Pair<String, String>>>> result = resultFuture.get(10000, TimeUnit.MILLISECONDS);

		String configLog = testAppender.events.stream()
				.filter(e -> ((String)e.getMessage()).trim().startsWith("##### Execution map"))
				.map(e -> ((String)e.getMessage()))
				.findFirst().get();
		Properties prop = new Properties();
		prop.load(new StringReader(configLog));
		
		assertTrue(prop.containsKey("dstream.source.hash"));
		assertTrue(prop.containsKey("dstream.source.probe"));
		
		assertTrue(configLog.contains("#dstream.output="));
		
		assertTrue(configLog.contains("#dstream.stage.parallelizm.0_hash="));
		assertTrue(configLog.contains("#dstream.stage.parallelizm.1_hash="));
		assertTrue(configLog.contains("#dstream.stage.parallelizm.0_probe="));
		assertTrue(configLog.contains("#dstream.stage.parallelizm.1_probe="));
		
		assertTrue(configLog.contains("#dstream.stage.ms_combine.0_hash="));
		assertTrue(configLog.contains("#dstream.stage.ms_combine.1_hash="));
		assertTrue(configLog.contains("#dstream.stage.ms_combine.0_probe="));
		assertTrue(configLog.contains("#dstream.stage.ms_combine.1_probe="));
		
		result.close();
	}
	
	public static class TestAppender extends AppenderSkeleton {
		public List<LoggingEvent> events = new ArrayList<LoggingEvent>();

		public void close() {
		}

		public boolean requiresLayout() {
			return false;
		}

		@Override
		protected void append(LoggingEvent event) {
			events.add(event);
		}
	}
}
