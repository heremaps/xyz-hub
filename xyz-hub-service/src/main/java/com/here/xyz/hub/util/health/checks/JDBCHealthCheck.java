/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.xyz.hub.util.health.checks;

import static com.here.xyz.hub.util.health.schema.Status.Result.ERROR;
import static com.here.xyz.hub.util.health.schema.Status.Result.OK;
import static com.here.xyz.hub.util.health.schema.Status.Result.UNKNOWN;

import com.here.xyz.hub.util.health.schema.Response;
import com.here.xyz.hub.util.health.schema.Status;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings("unused")
public class JDBCHealthCheck extends DBHealthCheck {

	protected Connection connection;
	protected String user;
	protected String password;
	
	public JDBCHealthCheck(URI connectionString, String user, String password) {
		super(connectionString);
		this.user = user;
		this.password = password;
	}
	
	public JDBCHealthCheck(String connectionStringEnvVar, String userEnvVar, String passwordEnvVar) throws URISyntaxException {
		super(new URI(System.getenv(connectionStringEnvVar)));
		this.user = System.getenv(userEnvVar);
		this.password = System.getenv(passwordEnvVar);
	}

	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			//ignore
		}
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			//ignore
		}
	}

	@Override
	public Status execute() {
		Status s = new Status();
		
		if (connectionString == null) {
			setResponse(new Response().withMessage("Can't do a JDBC health check. Connection string is missing."));
			return s.withResult(UNKNOWN);
		}
		
		//Create / check the connection
		try {
			if (connection == null || !connection.isValid(timeout / 1000)) {
				connection = DriverManager.getConnection(connectionString.toString(), user, password);
			}
		}
		catch (SQLException e) {
			setResponse(new Response().withMessage("Error when trying to establish the JDBC connection."));
			return s.withResult(ERROR);
		}
		
		//Do a simple query
		Statement statement;
		String simpleQuery = "SELECT 1";
		try {
			statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(simpleQuery);
			rs.next();
			int result = rs.getInt(1);
			if (result != 1) {
				setResponse(new Response().withMessage("Invalid result for sample query " + result + ". (Query: \"" + simpleQuery + "\")"));
				s.setResult(ERROR);
			}
			else {
				setResponse(null);
				s.setResult(OK);
			}
		}
		catch (SQLException e) {
			setResponse(new Response().withMessage("Error when trying to do execute sample query. (Query: \"" + simpleQuery + "\")"));
			return s.withResult(ERROR);
		}
		
		return s;
	}
	
}
