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

import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.PERMANENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.datasource.SingleConnectionDataSources;
import com.here.xyz.util.db.pg.Script;
import com.here.xyz.util.db.pg.ScriptInstaller;
import com.here.xyz.util.db.pg.ScriptInstaller.Outcome;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The script installation on a database on which the H3 extensions can not be installed.
 * The test uses a database of its own: the shared test database has the extensions, and whether an extension is available is a
 * property of the server installation, which a test can not change. So the test lets the installation of h3 fail instead, with a
 * function of the same name that exists already in h3_ext. Afterwards the database is in the same state as one without h3: no
 * h3_ext.h3index.
 */
public class PSQLH3InstallIT {
  private static final String HOST = "localhost";
  private static final String USER = "postgres";
  private static final String PASSWORD = "password";
  private static final String DB = PSQLH3InstallIT.class.getSimpleName().toLowerCase() + "_" + Random.randomAlpha(5).toLowerCase();
  private static final String VERSION = "1.0.0";
  private static final String PREFIX = "hub";
  private static Outcome outcome;

  @BeforeAll
  public static void installScripts() throws Exception {
    try (Connection connection = connect("postgres"); Statement statement = connection.createStatement()) {
      //template0, because template1 might have extensions installed
      statement.execute("CREATE DATABASE " + DB + " TEMPLATE template0");
    }
    try (Connection connection = connect(DB); Statement statement = connection.createStatement()) {
      //CREATE EXTENSION h3 fails on this function with SQLSTATE 55000, which none of the specific handlers of common.sql catches
      statement.execute("CREATE SCHEMA h3_ext");
      statement.execute("CREATE FUNCTION h3_ext.h3_get_extension_version() RETURNS text LANGUAGE sql AS 'SELECT ''none'''");
    }
    try (SingleConnectionDataSources dsp = newProvider()) {
      outcome = ScriptInstaller.checkAndInstall(List.of(new Script("/sql/common.sql", dsp, VERSION, PREFIX),
          new Script("/sql/hexbin_h3.sql", dsp, VERSION, PREFIX)), dsp, VERSION, 5, 120, null, Set.of(), "public");
    }
  }

  @AfterAll
  public static void dropDatabase() throws Exception {
    try (Connection connection = connect("postgres"); Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + DB + " WITH (FORCE)");
    }
  }

  @Test
  public void commonIsInstalledAlthoughTheExtensionsFail() throws Exception {
    assertEquals(PERMANENT, outcome.state());
    assertEquals(Set.of(PREFIX + ".hexbin_h3"), outcome.permanentlyFailedScripts(), "Only hexbin_h3.sql may fail");
    assertTrue(outcome.schemas().contains(PREFIX + ".common:" + VERSION), outcome.schemas().toString());
    //The block of common.sql is rolled back as a whole, so it leaves no h3 and no postgis_raster behind
    assertEquals(0L, querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname IN ('h3', 'h3_postgis', 'postgis_raster')"));
    assertEquals(1L, querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname = 'postgis'"));
  }

  @Test
  public void hexbinH3FailsPermanentlyAndStopsTheTemplateCheck() {
    assertFalse(outcome.schemas().contains(PREFIX + ".hexbin_h3:" + VERSION), outcome.schemas().toString());
    //The known trade-off: while a script is missing, the installer neither checks the template nor cleans up old versions
    assertEquals(TemplateCheck.FAILED_PERMANENT, outcome.templateChecks().values().iterator().next());
  }

  @Test
  public void h3QueryIsNotImplemented() {
    //The h3Index filter of GetFeaturesByGeometry, with the search path of the connections
    SQLException e = assertThrows(SQLException.class, () -> querySingle(PooledDataSources.compileSearchPath("public",
        outcome.schemas()), "SELECT h3_ext.h3_cell_to_boundary_geometry(xyz_h3_cell(?))", "871faeba8ffffff"));
    assertEquals("42883", e.getSQLState(), e.getMessage());
    ErrorResponseException mapped = assertThrows(ErrorResponseException.class,
        () -> new PSQLXyzConnector().handleProcessingException(e, new GetFeaturesByGeometryEvent().withSpace("test-space")));
    assertEquals(NOT_IMPLEMENTED, mapped.getErrorResponse().getError());
    assertEquals("H3 is not available on this storage.", mapped.getErrorResponse().getErrorMessage());
  }

  private static Connection connect(String db) throws SQLException {
    return DriverManager.getConnection("jdbc:postgresql://" + HOST + ":5432/" + db, USER, PASSWORD);
  }

  private static SingleConnectionDataSources newProvider() {
    return new SingleConnectionDataSources(new DatabaseSettings(PSQLH3InstallIT.class.getSimpleName())
        .withApplicationName(PSQLH3InstallIT.class.getSimpleName())
        .withHost(HOST)
        .withPort(5432)
        .withDb(DB)
        .withUser(USER)
        .withPassword(PASSWORD), 10, 180);
  }

  private static Object querySingle(String searchPath, String sql, String... parameters) throws SQLException {
    try (Connection connection = connect(DB)) {
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
}
