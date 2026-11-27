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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.Random;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Proves that installing the extensions h3 and h3_postgis (common.sql) does not break the PL/pgSQL H3 port which older versions
 * still use from their hub.h3:&lt;version&gt; and jobs.h3:&lt;version&gt; schemas.
 * legacy/h3.sql is the port as it was installed before. Remove it together with this test, once the old schemas are dropped.
 */
public class PSQLH3CoexistenceIT extends PSQLAbstractIT {
  //Mirrors the name of the versioned schema hub.h3:<version>
  private static final String LEGACY_SCHEMA = PSQLH3CoexistenceIT.class.getSimpleName().toLowerCase() + "_"
      + Random.randomAlpha(5).toLowerCase() + ".h3:4.0.0";
  private static final String CELL = "871faeba8ffffff";

  @BeforeAll
  public static void installLegacyPort() throws Exception {
    //initEnv() of PSQLAbstractIT installed the scripts, including the extension block of common.sql
    assertEquals(2L, querySingle(null, "SELECT count(*) FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace "
        + "WHERE e.extname IN ('h3', 'h3_postgis') AND n.nspname = 'h3_ext'"));
    //Nothing of the extensions may be in "public", where it would replace the domain h3index of the port
    assertEquals(0L, querySingle(null, "SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
        + "WHERE n.nspname = 'public' AND t.typname = 'h3index'"));
    assertEquals(0L, querySingle(null, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        + "WHERE n.nspname = 'public' AND p.proname LIKE 'h3%'"));
    assertEquals(0L, querySingle(null, "SELECT count(*) FROM pg_namespace WHERE nspname = 'h3'"));

    String script;
    try (InputStream is = PSQLH3CoexistenceIT.class.getResourceAsStream("/legacy/h3.sql")) {
      assertNotNull(is);
      script = new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection()) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA " + quote(LEGACY_SCHEMA));
        //Like the script installer: the target schema first, then "public"
        statement.execute("SET LOCAL search_path = " + quote(LEGACY_SCHEMA) + ", public");
        statement.execute(script);
        connection.commit();
      }
      catch (SQLException e) {
        connection.rollback();
        throw e;
      }
      finally {
        connection.setAutoCommit(true);
      }
    }
  }

  @AfterAll
  public static void dropLegacyPort() throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DROP SCHEMA IF EXISTS " + quote(LEGACY_SCHEMA) + " CASCADE");
    }
  }

  @Test
  public void geoToH3DegStillWorks() throws Exception {
    //geoToH3() uses 0::h3index and geoToH3Deg() declares a variable of type H3Index, both resolved through the search path
    String sql = "SELECT to_hex(geoToH3Deg(ST_SetSRID(ST_MakePoint(8.5131, 50.2141), 4326), 7))";
    assertEquals(CELL, querySingle(legacySearchPath(), sql));
    assertEquals(CELL, querySingle(null, "SELECT (ST_SetSRID(ST_MakePoint(8.5131, 50.2141), 4326) OPERATOR(h3_ext.@) 7)::text"));
  }

  @Test
  public void h3ToGeoBoundaryDegStillWorks() throws Exception {
    String expected = (String) querySingle(null, "SELECT ST_AsText(h3_ext.h3_cell_to_boundary_geometry('" + CELL + "'))");
    double distance = (Double) querySingle(legacySearchPath(), "SELECT ST_HausdorffDistance(ST_Force2D(ST_SetSRID("
        + "h3ToGeoBoundaryDeg(('x' || '" + CELL + "')::bit(60)::bigint), 4326)), ST_GeomFromText('" + expected + "', 4326))");
    assertTrue(distance < 1e-9, "The boundaries differ by " + distance);
  }

  @Test
  public void coveringDegStillWorks() throws Exception {
    //Polygons go through polyfillDeg()
    long cells = (Long) querySingle(legacySearchPath(),
        "SELECT count(*) FROM coveringDeg(ST_MakeEnvelope(8.50, 50.20, 8.53, 50.23, 4326), 7)");
    assertTrue(cells > 0);
  }

  //The search path of an older version: without h3_ext, the port's schema before hub.ext
  private static String legacySearchPath() {
    return "public, public, topology, \"hub.common\", \"hub.geo\", \"hub.feature_writer\", " + quote(LEGACY_SCHEMA) + ", \"hub.ext\"";
  }

  /**
   * Runs the query in a transaction which is rolled back, with the provided search path or the one of the connector's connections.
   */
  private static Object querySingle(String searchPath, String sql) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection()) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        if (searchPath != null)
          statement.execute("SET LOCAL search_path = " + searchPath);
        try (PreparedStatement query = connection.prepareStatement(sql); ResultSet result = query.executeQuery()) {
          assertTrue(result.next());
          return result.getObject(1);
        }
      }
      finally {
        connection.rollback();
        connection.setAutoCommit(true);
      }
    }
  }

  private static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
