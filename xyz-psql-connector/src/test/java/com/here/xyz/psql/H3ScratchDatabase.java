/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.psql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.SingleConnectionDataSources;
import com.here.xyz.util.db.pg.Script;
import com.here.xyz.util.db.pg.ScriptInstaller;
import com.here.xyz.util.db.pg.ScriptInstaller.Outcome;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A database of its own on the local test server, for the tests of the H3 parts of the script installation.
 * Such tests need a database of their own, because they change the extensions or create schemas with the names the installer
 * uses, which would interfere with the other tests on the shared test database.
 */
final class H3ScratchDatabase {
  static final String PREFIX = "hub";
  private static final String HOST = "localhost";
  private static final String USER = "postgres";
  private static final String PASSWORD = "password";
  private final String name;

  private H3ScratchDatabase(String name) {
    this.name = name;
  }

  /**
   * Creates an empty database with a unique name from template0, because template1 might have extensions installed.
   */
  static H3ScratchDatabase create(Class<?> testClass) throws SQLException {
    H3ScratchDatabase db = new H3ScratchDatabase(testClass.getSimpleName().toLowerCase() + "_" + Random.randomAlpha(5).toLowerCase());
    try (Connection connection = connect("postgres"); Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE " + db.name + " TEMPLATE template0");
    }
    return db;
  }

  String name() {
    return name;
  }

  /**
   * Installs common.sql and hexbin_h3.sql with the prefix "hub", as the connector does.
   */
  Outcome install(String version) {
    try (SingleConnectionDataSources dsp = new SingleConnectionDataSources(new DatabaseSettings(name)
        .withApplicationName(H3ScratchDatabase.class.getSimpleName())
        .withHost(HOST)
        .withPort(5432)
        .withDb(name)
        .withUser(USER)
        .withPassword(PASSWORD), 10, 180)) {
      return ScriptInstaller.checkAndInstall(List.of(new Script("/sql/common.sql", dsp, version, PREFIX),
          new Script("/sql/hexbin_h3.sql", dsp, version, PREFIX)), dsp, version, 5, 120, null, Set.of(), "public");
    }
  }

  /**
   * Runs the statements one by one, each in a transaction of its own.
   */
  void execute(String... statements) throws SQLException {
    try (Connection connection = connect(name); Statement statement = connection.createStatement()) {
      for (String sql : statements)
        statement.execute(sql);
    }
  }

  /**
   * Returns the first column of the first row, with the provided search path or the default one.
   */
  Object querySingle(String searchPath, String sql, String... parameters) throws SQLException {
    try (Connection connection = connect(name)) {
      if (searchPath != null)
        try (Statement statement = connection.createStatement()) {
          statement.execute("SET search_path = " + searchPath);
        }
      try (PreparedStatement query = connection.prepareStatement(sql)) {
        for (int i = 0; i < parameters.length; i++)
          query.setString(i + 1, parameters[i]);
        try (ResultSet result = query.executeQuery()) {
          assertTrue(result.next());
          return result.getObject(1);
        }
      }
    }
  }

  /**
   * Returns the names of all schemas except the ones of the system.
   */
  Set<String> schemas() throws SQLException {
    Set<String> schemas = new HashSet<>();
    try (Connection connection = connect(name); Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT nspname FROM pg_namespace "
            + "WHERE nspname NOT LIKE 'pg\\_%' AND nspname != 'information_schema'")) {
      while (result.next())
        schemas.add(result.getString(1));
    }
    return schemas;
  }

  void drop() throws SQLException {
    try (Connection connection = connect("postgres"); Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
    }
  }

  private static Connection connect(String db) throws SQLException {
    return DriverManager.getConnection("jdbc:postgresql://" + HOST + ":5432/" + db, USER, PASSWORD);
  }
}
