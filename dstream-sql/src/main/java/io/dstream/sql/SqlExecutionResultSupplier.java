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
package io.dstream.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.dstream.DStreamConstants;
import io.dstream.support.AbstractPartitionedStreamProducingSourceSupplier;
import io.dstream.support.SourceSupplier;
import io.dstream.utils.Assert;

/**
 * Implementation of the {@link SourceSupplier} which executes SQL statement and returns a
 * {@link Stream} of {@link Row}s
 *
 */
public class SqlExecutionResultSupplier extends AbstractPartitionedStreamProducingSourceSupplier<Row>{
	private static final long serialVersionUID = -7627364015734396132L;

	private Logger logger = Logger.getLogger(SqlExecutionResultSupplier.class.getName());

	public SqlExecutionResultSupplier(Properties executionConfig, String executionGraphName) {
		super(executionConfig, executionGraphName);
	}

	/**
	 *
	 */
	@Override
	public Stream<Row> get() {
		String sql = executionConfig.getProperty(DStreamConstants.SOURCE + executionGraphName);
		Assert.notEmpty(sql, "'" + (DStreamConstants.SOURCE + this.executionGraphName) +  "' property can not be found in execution configuration file.");

		String driver = this.executionConfig.getProperty(DStreamSQLConstants.SQL_DRIVER + this.executionGraphName);
		Assert.notEmpty(driver, "'" + (DStreamSQLConstants.SQL_DRIVER + this.executionGraphName) +  "' property can not be found in execution configuration file.");

		String url = this.executionConfig.getProperty(DStreamSQLConstants.SQL_URL + this.executionGraphName);
		Assert.notEmpty(url, "'" + (DStreamSQLConstants.SQL_URL + this.executionGraphName) +  "' property can not be found in execution configuration file.");

		Connection connection = null;
		try {
			Class.forName(driver, false, Thread.currentThread().getContextClassLoader());
			connection = DriverManager.getConnection(url);
			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			Connection c = connection;
			Iterator<Row> rowIterator = new Iterator<Row>() {

				@Override
				public boolean hasNext() {
					try {
						boolean hasNext = rs.next();
						if (!hasNext){
							logger.info("Finished processing query for " + url);
							close(rs, c);
						}
						return hasNext;
					} catch (Exception e) {

						close(rs, c);
						throw new IllegalStateException(e);
					}
				}

				@Override
				public Row next() {
					List<Object> columnValues = new ArrayList<>();
					try {
						for (int i = 1; i <= rsmd.getColumnCount(); i++) {
							columnValues.add(rs.getObject(i));
						}
					} catch (Exception e) {
						close(rs, c);
						throw new IllegalStateException(e);
					}
					return new DefaultRowImpl(columnValues.toArray());
				}
			};
			Stream<Row> targetStream = StreamSupport
					.stream(Spliterators.spliteratorUnknownSize(rowIterator, Spliterator.ORDERED),false);
			return targetStream;
		}
		catch (Exception e) {
			this.close(connection);
			throw new IllegalStateException("Failed while performing SQL operation", e);
		}
	}

	/**
	 *
	 */
	private void close(AutoCloseable... c){
		logger.info("Closing DB resources");
		for (AutoCloseable closeable : c) {
			try {
				closeable.close();
			} catch (Exception _e) {/*ignore*/}
		}
	}
}
