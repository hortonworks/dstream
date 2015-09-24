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

import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;

/**
 *
 */
public class ExecutionContextAwareTezClient extends TezClient {
	private final  Map<String, LocalResource> localResources;

	private final TezConfiguration tezConfiguration;

	private final FileSystem fileSystem;

	/**
	 *
	 * @param name
	 * @param tezConf
	 * @param localResources
	 * @param credentials
	 * @param fileSystem
	 */
	public ExecutionContextAwareTezClient(String name,
			TezConfiguration tezConf,
			Map<String, LocalResource> localResources,
			Credentials credentials,
			FileSystem fileSystem) {

		super(name, tezConf, tezConf.getBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, TezConfiguration.TEZ_AM_SESSION_MODE_DEFAULT), localResources, credentials);
		this.tezConfiguration = tezConf;
		this.localResources = localResources;
		this.fileSystem = fileSystem;
	}

	/**
	 *
	 * @return
	 */
	public Map<String, LocalResource> getLocalResources() {
		return localResources;
	}

	/**
	 * current Tez configuration as {@link TezConfiguration}
	 * @return current Tez configuration
	 */
	public TezConfiguration getTezConfiguration() {
		return tezConfiguration;
	}

	/**
	 *
	 * @return
	 */
	public FileSystem getFileSystem() {
		return fileSystem;
	}
}
