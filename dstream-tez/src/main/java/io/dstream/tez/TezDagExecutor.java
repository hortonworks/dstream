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
package io.dstream.tez;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * It implements {@link Callable} so it could be submitted to the {@link Executor} async.
 *
 */
public class TezDagExecutor implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(TezDagExecutor.class);

	private final ExecutionContextAwareTezClient tezClient;

	private final DAG dag;

	/**
	 *
	 * @param tezClient
	 * @param dag
	 */
	public TezDagExecutor(ExecutionContextAwareTezClient tezClient, DAG dag) {
		this.tezClient = tezClient;
		this.dag = dag;
	}

	/**
	 *
	 */
	@Override
	public void run() {
		if (logger.isInfoEnabled()){
			logger.info("Constructed Tez DAG " + this.dag.getName());
		}

		try {
			tezClient.waitTillReady();

			if (logger.isInfoEnabled()){
				logger.info("Submitting generated DAG to YARN/Tez cluster");
			}

			DAGClient dagClient = this.tezClient.submitDAG(this.dag);
			DAGStatus dagStatus =  dagClient.waitForCompletionWithStatusUpdates(null);

			if (logger.isInfoEnabled()){
				logger.info("DAG execution complete");
			}
			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				logger.error("DAG diagnostics: " + dagStatus.getDiagnostics());
			}
			dagClient.close();
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to execute Tez DAG", e);
		}
	}
}
