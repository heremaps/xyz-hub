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
import static com.here.xyz.util.db.pg.ScriptInstaller.State.VERIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.pg.ScriptInstaller.Outcome;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The script installation drops the schemas of the old PL/pgSQL H3 port (hub.h3, hub.h3:&lt;version&gt;, jobs.h3 and
 * jobs.h3:&lt;version&gt;) on an existing database, and nothing else.
 * The test uses a database of its own, because the fixtures have the names of real schemas, which the installations of other tests
 * on the shared test database would drop or depend on.
 */
public class PSQLH3SchemaCleanupIT {
  //The schemas of the old port, to be dropped. The last one proves the quoting of the schema name.
  private static final List<String> OLD = List.of("hub.h3", "hub.h3:4.0.1", "jobs.h3", "jobs.h3:4.0.1", "jobs.h3:4.0.1\"q");
  //Schemas with similar names, to be kept
  private static final List<String> KEPT = List.of("hub.hexbin_h3:4.0.1", "jobs.hexbin_h3", "jobs.hexbin_h3:4.0.1", "h3", "m499_h3",
      "hub.h3:", "hub.h3x", "xhub.h3");
  //A schema of the old port with an object outside of it depending on it, to be kept
  private static final String GUARDED = "hub.h3:0.0.1";
  private static final String DEPENDENT = "h3_port_dependent";
  private static final String CELL = "871faeba8ffffff";
  private static final Map<String, Long> functionsBefore = new HashMap<>();
  private static final Map<String, Long> oidsBefore = new HashMap<>();
  private static H3ScratchDatabase db;
  private static Outcome outcome2;
  private static Outcome outcome3;
  private static Set<String> after2;
  private static Set<String> after3;

  @BeforeAll
  public static void installScripts() throws Exception {
    db = H3ScratchDatabase.create(PSQLH3SchemaCleanupIT.class);
    //The database is new, so this installation creates the extensions
    assertEquals(VERIFIED, db.install("4.1.0").state());

    for (String schema : OLD)
      createPortLikeSchema(schema);
    for (String schema : KEPT)
      createPortLikeSchema(schema);
    createPortLikeSchema(GUARDED);
    db.execute("CREATE SCHEMA " + DEPENDENT, "CREATE FUNCTION " + DEPENDENT + ".keep_me(h " + quote(GUARDED) + ".h3index) RETURNS int "
        + "LANGUAGE sql AS 'SELECT 1'");

    for (String schema : KEPT)
      functionsBefore.put(schema, functionCount(schema));
    for (String extension : List.of("h3", "h3_postgis", "postgis_raster"))
      oidsBefore.put(extension, extensionOid(extension));
    oidsBefore.put("h3_ext", schemaOid("h3_ext"));

    //A new version, so that common.sql gets installed again, as it is after the deployment of a new release
    outcome2 = db.install("4.1.1");
    after2 = db.schemas();
    outcome3 = db.install("4.1.2");
    after3 = db.schemas();
  }

  @AfterAll
  public static void dropDatabase() throws Exception {
    if (db != null)
      db.drop();
  }

  @Test
  public void oldPortSchemasAreDropped() {
    for (String schema : OLD)
      assertFalse(after2.contains(schema), schema);
  }

  @Test
  public void otherSchemasAreKept() throws Exception {
    for (String schema : List.of(PREFIX + ".hexbin_h3", "h3_ext", DEPENDENT))
      assertTrue(after2.contains(schema), schema);
    for (String schema : KEPT) {
      assertTrue(after2.contains(schema), schema);
      assertEquals(functionsBefore.get(schema), functionCount(schema), schema);
    }
    assertNotNull(db.querySingle(null, "SELECT to_regtype('m499_h3.h3index')::text"));
    assertNotNull(db.querySingle(null, "SELECT to_regtype('h3.h3index')::text"));
  }

  @Test
  public void schemaWithOutsideDependentsIsKept() throws Exception {
    assertTrue(after2.contains(GUARDED));
    assertNotNull(db.querySingle(null, "SELECT to_regprocedure(?)::text", DEPENDENT + ".keep_me(" + quote(GUARDED) + ".h3index)"));
  }

  @Test
  public void extensionsAreUntouched() throws Exception {
    for (String extension : List.of("h3", "h3_postgis", "postgis_raster"))
      assertEquals(oidsBefore.get(extension), extensionOid(extension), extension);
    assertEquals(oidsBefore.get("h3_ext"), schemaOid("h3_ext"));
    assertEquals(2L, db.querySingle(null, "SELECT count(*) FROM pg_extension "
        + "WHERE extname IN ('h3', 'h3_postgis') AND extnamespace = 'h3_ext'::regnamespace"));
    assertEquals(1L, db.querySingle(null, "SELECT count(*) FROM pg_extension "
        + "WHERE extname = 'postgis_raster' AND extnamespace = 'public'::regnamespace"));
  }

  @Test
  public void h3QueriesStillWork() throws Exception {
    String searchPath = PooledDataSources.compileSearchPath("public", outcome2.schemas());
    long cells = (Long) db.querySingle(searchPath, "SELECT count(*) FROM xyz_h3_cover(ST_MakeEnvelope(8.50, 50.20, 8.53, 50.23, 4326), 7)");
    assertTrue(cells > 0);
    assertNotNull(db.querySingle(searchPath, "SELECT ST_AsText(h3_ext.h3_cell_to_boundary_geometry(xyz_h3_cell(?)))", CELL));
    assertEquals(CELL, db.querySingle(searchPath,
        "SELECT (ST_SetSRID(ST_MakePoint(8.5131, 50.2141), 4326) OPERATOR(h3_ext.@) 7)::text"));
  }

  @Test
  public void secondInstallationIsANoOp() {
    assertEquals(VERIFIED, outcome2.state());
    assertEquals(VERIFIED, outcome3.state());
    Set<String> newSchemas = new HashSet<>(after3);
    newSchemas.removeAll(Set.of(PREFIX + ".common:4.1.2", PREFIX + ".hexbin_h3:4.1.2"));
    assertEquals(after2, newSchemas);
  }

  /**
   * Creates a schema with objects like the ones of the port: a domain h3index, a composite type and functions using both.
   */
  private static void createPortLikeSchema(String schema) throws SQLException {
    String s = quote(schema);
    db.execute("CREATE SCHEMA " + s,
        "CREATE DOMAIN " + s + ".h3index AS bigint",
        "CREATE TYPE " + s + ".face_ijk_t AS (face int, i int, j int, k int)",
        "CREATE FUNCTION " + s + ".h3_version() RETURNS text LANGUAGE sql AS 'SELECT ''3.2.0'''",
        "CREATE FUNCTION " + s + ".geotoh3(f " + s + ".face_ijk_t) RETURNS " + s + ".h3index LANGUAGE sql "
            + "AS 'SELECT 0::bigint::" + s + ".h3index'");
  }

  private static long functionCount(String schema) throws SQLException {
    return (Long) db.querySingle(null, "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        + "WHERE n.nspname = ?", schema);
  }

  private static Long extensionOid(String extension) throws SQLException {
    return (Long) db.querySingle(null, "SELECT oid::bigint FROM pg_extension WHERE extname = ?", extension);
  }

  private static Long schemaOid(String schema) throws SQLException {
    return (Long) db.querySingle(null, "SELECT oid::bigint FROM pg_namespace WHERE nspname = ?", schema);
  }

  private static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
