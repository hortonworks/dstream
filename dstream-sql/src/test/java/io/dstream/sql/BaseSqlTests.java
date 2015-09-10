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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

/**
 *
 */
public class BaseSqlTests {

	public final String url = "jdbc:derby:sampleDB;create=true;";

	@Before
	public void before() throws Exception {
		System.out.println("Preparing DB");
		String driver = "org.apache.derby.jdbc.EmbeddedDriver";
		Class.forName(driver);
		Connection c = DriverManager.getConnection(this.url);
		try {
			c.prepareStatement("DROP TABLE EMPLOYEE").executeUpdate();
		} catch (Exception e) {
			// ignore
		}
		PreparedStatement create = c.prepareStatement("CREATE TABLE EMPLOYEE ("
				+ "EMP_ID INT NOT NULL, "
				+ "HIRE_DATE DATE NOT NULL, "
				+ "EMP_NAME VARCHAR(20) NOT NULL, PRIMARY KEY (EMP_ID))");
		create.executeUpdate();

		c.prepareStatement("INSERT INTO EMPLOYEE (EMP_ID, HIRE_DATE, EMP_NAME) VALUES (0, DATE('1994-02-23'), 'John Doe')").executeUpdate();
		c.prepareStatement("INSERT INTO EMPLOYEE (EMP_ID, HIRE_DATE, EMP_NAME) VALUES (1, DATE('2013-05-03'), 'Steve Smith')").executeUpdate();
		c.prepareStatement("INSERT INTO EMPLOYEE (EMP_ID, HIRE_DATE, EMP_NAME) VALUES (2, DATE('2013-02-13'), 'Steve Rogers')").executeUpdate();
		c.prepareStatement("INSERT INTO EMPLOYEE (EMP_ID, HIRE_DATE, EMP_NAME) VALUES (3, DATE('2000-03-26'), 'Stacy Rodriguez')").executeUpdate();
		c.prepareStatement("INSERT INTO EMPLOYEE (EMP_ID, HIRE_DATE, EMP_NAME) VALUES (4, DATE('2001-01-30'), 'Camila Wilson')").executeUpdate();
	}

	@After
	public void after() throws Exception {
		FileUtils.deleteDirectory(new File("sampleDB"));
	}
}
