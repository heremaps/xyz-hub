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

import static com.here.xyz.psql.H3ScratchDatabase.PREFIX;
import static com.here.xyz.responses.XyzError.NOT_IMPLEMENTED;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.PERMANENT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.VERIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.pg.ScriptInstaller.Outcome;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The installation of the H3 extensions by common.sql, which creates them only on a new database. Each scenario uses a database of
 * its own, because it changes the extensions, which the other tests on the shared test database need.
 * <ul>
 *   <li>"fresh": a new database gets the extensions, and installing again changes nothing.</li>
 *   <li>"failing": a new database on which the H3 extensions can not be installed. Whether an extension is available is a property
 *   of the server installation, which a test can not change. So the test lets the installation of h3 fail instead, with a function
 *   of the same name that exists already in h3_ext. The H3 part of the block is rolled back as a whole, so afterwards the database
 *   is in the same state as one without h3: postgis, but no postgis_raster, h3 or h3_postgis, and no h3_ext.h3index.</li>
 *   <li>"existing without h3": an existing database without the extensions does not get them.</li>
 * </ul>
 */
public class PSQLH3InstallIT {
  private static final String VERSION = "1.0.0";
  private static final List<String> EXTENSIONS = List.of("h3", "h3_postgis", "postgis_raster");
  private static final Map<String, Long> freshOids = new HashMap<>();
  private static H3ScratchDatabase failingDb;
  private static H3ScratchDatabase freshDb;
  private static H3ScratchDatabase existingDb;
  private static Outcome outcome;
  private static Outcome freshOutcome;
  private static Outcome freshOutcome2;
  private static Outcome existingOutcome;

  @BeforeAll
  public static void installScripts() throws Exception {
    failingDb = H3ScratchDatabase.create(PSQLH3InstallIT.class);
    //CREATE EXTENSION h3 fails on this function with SQLSTATE 55000, which none of the specific handlers of common.sql catches
    failingDb.execute("CREATE SCHEMA h3_ext",
        "CREATE FUNCTION h3_ext.h3_get_extension_version() RETURNS text LANGUAGE sql AS 'SELECT ''none'''");
    outcome = failingDb.install(VERSION);

    freshDb = H3ScratchDatabase.create(PSQLH3InstallIT.class);
    freshOutcome = freshDb.install("4.1.0");
    for (String extension : EXTENSIONS)
      freshOids.put(extension, extensionOid(freshDb, extension));
    freshOids.put("h3_ext", (Long) freshDb.querySingle(null, "SELECT oid::bigint FROM pg_namespace WHERE nspname = 'h3_ext'"));
    freshOutcome2 = freshDb.install("4.1.1");

    existingDb = H3ScratchDatabase.create(PSQLH3InstallIT.class);
    assertEquals(VERIFIED, existingDb.install("4.1.0").state());
    existingDb.execute("DROP EXTENSION h3_postgis, h3 CASCADE", "DROP SCHEMA h3_ext CASCADE");
    existingOutcome = existingDb.install("4.1.1");
  }

  @AfterAll
  public static void dropDatabases() throws Exception {
    //Each one on its own, so that one failure does not leak the other databases
    try {
      if (failingDb != null)
        failingDb.drop();
    }
    finally {
      try {
        if (freshDb != null)
          freshDb.drop();
      }
      finally {
        if (existingDb != null)
          existingDb.drop();
      }
    }
  }

  @Test
  public void freshDatabaseGetsTheExtensions() throws Exception {
    assertEquals(VERIFIED, freshOutcome.state());
    assertTrue(freshOutcome.schemas().contains(PREFIX + ".hexbin_h3:4.1.0"), freshOutcome.schemas().toString());
    assertEquals(1L, freshDb.querySingle(null, "SELECT count(*) FROM pg_extension "
        + "WHERE extname = 'postgis_raster' AND extnamespace = 'public'::regnamespace"));
    assertEquals(2L, freshDb.querySingle(null, "SELECT count(*) FROM pg_extension "
        + "WHERE extname IN ('h3', 'h3_postgis') AND extnamespace = 'h3_ext'::regnamespace"));
    assertEquals(true, freshDb.querySingle(null, "SELECT has_schema_privilege('public', 'h3_ext', 'USAGE')"));
  }

  @Test
  public void freshDatabaseHasNothingOfH3InPublic() throws Exception {
    assertEquals(0L, freshDb.querySingle(null, "SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace "
        + "WHERE n.nspname = 'public' AND t.typname = 'h3index'"));
    assertEquals(0L, freshDb.querySingle(null, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        + "WHERE n.nspname = 'public' AND p.proname LIKE 'h3%'"));
    assertEquals(0L, freshDb.querySingle(null, "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace "
        + "WHERE n.nspname = 'public' AND 'h3_ext.h3index'::regtype IN (o.oprleft, o.oprright)"));
    assertEquals(0L, freshDb.querySingle(null, "SELECT count(*) FROM pg_namespace WHERE nspname = 'h3'"));
  }

  @Test
  public void reinstallationKeepsTheExtensions() throws Exception {
    assertEquals(VERIFIED, freshOutcome2.state());
    for (String extension : EXTENSIONS)
      assertEquals(freshOids.get(extension), extensionOid(freshDb, extension), extension);
    assertEquals(freshOids.get("h3_ext"), freshDb.querySingle(null, "SELECT oid::bigint FROM pg_namespace WHERE nspname = 'h3_ext'"));
  }

  @Test
  public void existingDatabaseGetsNoExtensions() throws Exception {
    assertEquals(0L, existingDb.querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname IN ('h3', 'h3_postgis')"));
    assertEquals(0L, existingDb.querySingle(null, "SELECT count(*) FROM pg_namespace WHERE nspname = 'h3_ext'"));
    assertEquals(1L, existingDb.querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname = 'postgis_raster'"));
    assertEquals(PERMANENT, existingOutcome.state());
    assertEquals(Set.of(PREFIX + ".hexbin_h3"), existingOutcome.permanentlyFailedScripts());
    assertTrue(existingOutcome.schemas().contains(PREFIX + ".common:4.1.1"), existingOutcome.schemas().toString());
  }

  @Test
  public void commonIsInstalledAlthoughTheExtensionsFail() throws Exception {
    assertEquals(PERMANENT, outcome.state());
    assertEquals(Set.of(PREFIX + ".hexbin_h3"), outcome.permanentlyFailedScripts(), "Only hexbin_h3.sql may fail");
    assertTrue(outcome.schemas().contains(PREFIX + ".common:" + VERSION), outcome.schemas().toString());
    //The H3 part of the block of common.sql is rolled back as a whole, so it leaves no h3 and no postgis_raster behind
    assertEquals(0L, failingDb.querySingle(null,
        "SELECT count(*) FROM pg_extension WHERE extname IN ('h3', 'h3_postgis', 'postgis_raster')"));
    assertEquals(1L, failingDb.querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname = 'postgis'"));
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
    SQLException e = assertThrows(SQLException.class, () -> failingDb.querySingle(PooledDataSources.compileSearchPath("public",
        outcome.schemas()), "SELECT h3_ext.h3_cell_to_boundary_geometry(xyz_h3_cell(?))", "871faeba8ffffff"));
    assertEquals("42883", e.getSQLState(), e.getMessage());
    ErrorResponseException mapped = assertThrows(ErrorResponseException.class,
        () -> new PSQLXyzConnector().handleProcessingException(e, new GetFeaturesByGeometryEvent().withSpace("test-space")));
    assertEquals(NOT_IMPLEMENTED, mapped.getErrorResponse().getError());
    assertEquals("H3 is not available on this storage.", mapped.getErrorResponse().getErrorMessage());
  }

  private static Long extensionOid(H3ScratchDatabase db, String extension) throws SQLException {
    return (Long) db.querySingle(null, "SELECT oid::bigint FROM pg_extension WHERE extname = ?", extension);
  }
}
