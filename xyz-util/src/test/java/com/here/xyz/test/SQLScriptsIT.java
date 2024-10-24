/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
      assertFalse("It's not expected that version 1.0.3 was installed, as its content is equal to the one from version 1.0.2",
          installedVersions.contains("1.0.3"));
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
      assertTrue("The kept script version should be the newest of the installed ones.", installedVersions.contains("1.0.2"));
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
