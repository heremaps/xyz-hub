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

package com.here.xyz.test.sql.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.pg.Script;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SQLScriptsIT extends SQLITBase {

  @AfterAll
  public static void cleanupAllVersions() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      Script functions = new Script("/functions0/functions.sql", dsp, "1.0.0");
      List<String> installedVersions = functions.listInstalledScriptVersions();
      List<String> schemas = new ArrayList<>(installedVersions.stream().map(version -> functions.getScriptId() + ":" + version).toList());
      schemas.add(functions.getScriptId());
      for (String schema : schemas)
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
    }
  }

  @BeforeEach
  public void initTest() throws Exception {
    cleanupAllVersions();
  }

  @Test
  public void installScriptsFromResourceFolder() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      //Install all scripts residing in the resource folder "sqlSamples"
      List<Script> scripts = Script.loadScripts(new ScriptResourcePath("/sqlSamples"), dsp, "1.0.1");
      scripts.forEach(script -> script.install());
    }

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      //Calling the function with its simple name should not work without the search path being enhanced
      testSampleFunctionCallNegative(dsp);
      //Call functions using their FQN for verification
      testSampleFunctionCallPositive(dsp, "functions");
      testSampleFunctionCallPositive(dsp, "functions:1.0.1");
    }

    try (DataSourceProvider dsp = getDataSourceProvider(((DatabaseSettings) DB_SETTINGS.copy()).withSearchPath(List.of("functions:1.0.1")))) {
      //Call the function with its simple name
      testSampleFunctionCallPositive(dsp, null);
    }
  }

  private static void testSampleFunctionCallPositive(DataSourceProvider dsp, String schema) throws SQLException {
    assertEquals("Hello World, TestUser", runSampleFunctionCall(dsp, schema));
  }

  private static String runSampleFunctionCall(DataSourceProvider dsp, String schema) throws SQLException {
    return new SQLQuery("SELECT " + (schema != null ? "${schema}." : "") + "myTestFunction(#{param})")
        .withVariable("schema", schema)
        .withNamedParameter("param", "TestUser")
        .run(dsp, rs -> rs.next() ? rs.getString(1) : null);
  }

  private static void testSampleFunctionCallNegative(DataSourceProvider dsp) {
    assertThrows("Expect function not to be found, because its schema is not in the search path of the connection",
        SQLException.class, () -> new SQLQuery("SELECT myTestFunction(#{param})")
            .withNamedParameter("param", "TestUser")
            .run(dsp));
  }

  @Test
  public void listInstalledScriptVersions() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      Script functions = installFunctionScriptVersions(dsp);

      List<String> installedVersions = functions.listInstalledScriptVersions();
      assertTrue(installedVersions.contains("1.0.0"));
      assertTrue(installedVersions.contains("1.0.1"));
      assertTrue(installedVersions.contains("1.0.2"));
      assertTrue("Version 1.0.3 is expected to be installed even though its content is equal to the one from version 1.0.2, "
          + "because #getCompatibleSchema() resolves the schema by version and would otherwise fall back to a version that "
          + "is not guaranteed to carry the same content", installedVersions.contains("1.0.3"));
    }
  }

  @Test
  public void cleanupScriptVersions() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      Script functions = installFunctionScriptVersions(dsp);

      functions.cleanupOldScriptVersions(1);
      List<String> installedVersions = functions.listInstalledScriptVersions();
      assertEquals("There should be kept only one script version after cleaning up old script versions", 1,
          installedVersions.size());
      assertTrue("The kept script version should be the newest of the installed ones.", installedVersions.contains("1.0.3"));
    }
  }

  @Test
  public void versionBumpWithoutContentChangeResolvesToTheDeployedContent() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      //An early software version installed some older content ...
      new Script("/functions0/functions.sql", dsp, "1.0.0").install();
      //... then a build reporting a version from a different (lower sorting) version scheme installed the current content.
      //That also wrote the current content into the unversioned "latest" schema, which is what the hash check compares against.
      new Script("/functions2/functions.sql", dsp, "0.0.623").install();

      //Now the same content gets deployed again, this time reporting a proper version. The content did not change, so only
      //the check for the missing versioned schema can trigger the installation here.
      Script deployed = new Script("/functions2/functions.sql", dsp, "1.0.1");
      deployed.install();

      assertEquals("The schema resolved for the deployed version must be the one of that very version", "functions:1.0.1",
          deployed.getCompatibleSchema());
      assertEquals("The resolved schema must carry the content of the deployed version, not the one of an older version",
          "Hello, TestUser", runSampleFunctionCall(dsp, "functions:1.0.1"));
    }
  }

  @Test
  public void useInstalledJsLib() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      new SQLQuery("""
          CREATE OR REPLACE FUNCTION jsonpath(document TEXT, path TEXT)
          RETURNS text AS
          $BODY$
            plv8.execute("SELECT require('jsonpath_rfc9535')");
            return jsonpath_rfc9535.query(JSON.parse(document), path)[0];
          $BODY$
          LANGUAGE 'plv8' IMMUTABLE
          PARALLEL UNSAFE;
          """).write(dsp);

      String returnedValue = new SQLQuery("SELECT jsonpath(#{document}, #{path})")
          .withVariable("schema", "hub.common")
          .withNamedParameter("document", """
              {
                "store": {
                  "book": [
                    {
                      "category": "reference",
                      "author": "Nigel Rees",
                      "title": "Sayings of the Century",
                      "price": 8.95
                    }
                  ]
                }
              }
              """)
          .withNamedParameter("path", "$.store.book[0].author")
          .run(dsp, rs -> rs.next() ? rs.getString(1) : null);

      assertEquals(returnedValue, "Nigel Rees");
    }
  }

  private static Script installFunctionScriptVersions(DataSourceProvider dsp) {
    Script functions100 = new Script("/functions0/functions.sql", dsp, "1.0.0");
    Script functions101 = new Script("/sqlSamples/functions.sql", dsp, "1.0.1");
    Script functions102 = new Script("/functions2/functions.sql", dsp, "1.0.2");
    Script functions103 = new Script("/functions2/functions.sql", dsp, "1.0.3");

    functions100.install();
    functions101.install();
    functions102.install();
    functions103.install();
    return functions103;
  }
}
