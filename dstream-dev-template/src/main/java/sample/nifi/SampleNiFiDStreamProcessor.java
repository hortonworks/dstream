/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sample.nifi;

import java.util.stream.Stream;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ProcessorLog;

import io.dstream.DStream;
import io.dstream.nifi.AbstractDStreamProcessor;

/**
 * Sample implementation of {@link AbstractDStreamProcessor} with its primary
 * goal to serve as an example of how to implement {@link DStream} NAR bundle
 * to be deployed in Apache NiFi.<br>
 */
@Tags({ "dstream", "SampleDStream"})
@CapabilityDescription("This processor loads and executes Sample DStream processes")
public class SampleNiFiDStreamProcessor extends AbstractDStreamProcessor {

	/**
	 *
	 */
	@Override
	protected <T> DStream<T> getDStream(String executionName) {
		final ProcessorLog log = this.getLogger();
		log.info("Recieved request for DStream '" + executionName + "'.");
		if (executionName.equals("WordCount")){
			return this.getWordCount();
		}
		throw new IllegalStateException("Failed to build DStream for execution '" + executionName + "'");
	}

	/**
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <T> DStream<T> getWordCount() {
		return (DStream<T>) DStream.ofType(String.class, "wc")
				.flatMap(record -> Stream.of(record.split("\\s+")))
				.reduceValues(word -> word, word -> 1, Integer::sum);
	}
}
