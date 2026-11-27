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
  private static final String VERSION = "1.0.0";
  private static H3ScratchDatabase db;
  private static Outcome outcome;

  @BeforeAll
  public static void installScripts() throws Exception {
    db = H3ScratchDatabase.create(PSQLH3InstallIT.class);
    //CREATE EXTENSION h3 fails on this function with SQLSTATE 55000, which none of the specific handlers of common.sql catches
    db.execute("CREATE SCHEMA h3_ext",
        "CREATE FUNCTION h3_ext.h3_get_extension_version() RETURNS text LANGUAGE sql AS 'SELECT ''none'''");
    outcome = db.install(VERSION);
  }

  @AfterAll
  public static void dropDatabase() throws Exception {
    if (db != null)
      db.drop();
  }

  @Test
  public void commonIsInstalledAlthoughTheExtensionsFail() throws Exception {
    assertEquals(PERMANENT, outcome.state());
    assertEquals(Set.of(PREFIX + ".hexbin_h3"), outcome.permanentlyFailedScripts(), "Only hexbin_h3.sql may fail");
    assertTrue(outcome.schemas().contains(PREFIX + ".common:" + VERSION), outcome.schemas().toString());
    //The block of common.sql is rolled back as a whole, so it leaves no h3 and no postgis_raster behind
    assertEquals(0L, db.querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname IN ('h3', 'h3_postgis', 'postgis_raster')"));
    assertEquals(1L, db.querySingle(null, "SELECT count(*) FROM pg_extension WHERE extname = 'postgis'"));
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
    SQLException e = assertThrows(SQLException.class, () -> db.querySingle(PooledDataSources.compileSearchPath("public",
        outcome.schemas()), "SELECT h3_ext.h3_cell_to_boundary_geometry(xyz_h3_cell(?))", "871faeba8ffffff"));
    assertEquals("42883", e.getSQLState(), e.getMessage());
    ErrorResponseException mapped = assertThrows(ErrorResponseException.class,
        () -> new PSQLXyzConnector().handleProcessingException(e, new GetFeaturesByGeometryEvent().withSpace("test-space")));
    assertEquals(NOT_IMPLEMENTED, mapped.getErrorResponse().getError());
    assertEquals("H3 is not available on this storage.", mapped.getErrorResponse().getErrorMessage());
  }
}
