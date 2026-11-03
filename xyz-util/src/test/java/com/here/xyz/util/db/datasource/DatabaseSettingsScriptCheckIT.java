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

package com.here.xyz.util.db.datasource;

import static com.here.xyz.util.Random.randomAlpha;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class DatabaseSettingsScriptCheckIT {
  private static final String PG_HOST = "localhost";
  private static final String PG_DB = "postgres";
  private static final String PG_USER = "postgres";
  private static final String PG_PW = "password";
  private static final String PREFIX = "it" + randomAlpha(5).toLowerCase();
  private static final String SCHEMA = PREFIX + ".functions";

  @AfterAll
  public static void dropSchemas() throws Exception {
    try (SingleConnectionDataSources dsp = new SingleConnectionDataSources(settings(newId(), PG_PW, 5432, false), 5, 20)) {
      List<String> schemas = new SQLQuery("SELECT nspname FROM pg_namespace WHERE nspname LIKE #{pattern}")
          .withNamedParameter("pattern", PREFIX + ".%")
          .run(dsp, rs -> {
            List<String> result = new ArrayList<>();
            while (rs.next())
              result.add(rs.getString(1));
            return result;
          });
      for (String schema : schemas)
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
    }
  }

  private static String newId() {
    return DatabaseSettingsScriptCheckIT.class.getSimpleName() + "_" + randomAlpha(5);
  }

  private static DatabaseSettings settings(String id, String password, int port) {
    return settings(id, password, port, true);
  }

  private static DatabaseSettings settings(String id, String password, int port, boolean withScripts) {
    DatabaseSettings settings = new DatabaseSettings(id)
        .withApplicationName(DatabaseSettingsScriptCheckIT.class.getSimpleName())
        .withHost(PG_HOST)
        .withPort(port)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(password);
    return withScripts ? settings.withScriptResourcePaths(List.of(new ScriptResourcePath("/sqlSamples", PREFIX))) : settings;
  }

  @Test
  public void installOnFirstUseAndSetTheSearchPath() throws Exception {
    DatabaseSettings settings = settings(newId(), PG_PW, 5432);
    try (PooledDataSources dsp = new PooledDataSources(settings)) {
      assertEquals(List.of(SCHEMA), settings.getSearchPath());
      assertEquals("The installed function must be found through the search path", "Hello World, TestUser",
          new SQLQuery("SELECT myTestFunction(#{param})")
              .withNamedParameter("param", "TestUser")
              .run(dsp, rs -> rs.next() ? rs.getString(1) : null));
    }
    assertNull(DatabaseSettings.getScriptCheckRetryAfter(settings.getId()));
  }

  @Test
  public void sameIdUsesTheResolvedSearchPathWithoutConnecting() throws Exception {
    String id = newId();
    DatabaseSettings first = settings(id, PG_PW, 5432);
    new PooledDataSources(first).close();
    assertEquals(List.of(SCHEMA), first.getSearchPath());

    //Settings with the same ID must use the resolved search path without connecting, which would fail with the wrong password
    DatabaseSettings second = settings(id, "wrongPassword", 5432);
    new PooledDataSources(second).close();
    assertEquals(first.getSearchPath(), second.getSearchPath());
    assertNull("A failed check would have delayed the next check", DatabaseSettings.getScriptCheckRetryAfter(id));
  }

  @Test
  public void unreachableDbFallsBackAndDelaysTheNextCheck() throws Exception {
    String id = newId();

    long start = System.currentTimeMillis();
    DatabaseSettings unreachable = settings(id, PG_PW, 1);
    new PooledDataSources(unreachable).close();
    long duration = System.currentTimeMillis() - start;
    assertTrue("The check must end within the connect timeout, took " + duration + " ms", duration < 7_000);
    assertEquals("The search path falls back to the unversioned schema", List.of(SCHEMA), unreachable.getSearchPath());
    Long retryAfter = DatabaseSettings.getScriptCheckRetryAfter(id);
    assertNotNull("A failed check must delay the next check", retryAfter);

    start = System.currentTimeMillis();
    DatabaseSettings again = settings(id, PG_PW, 1);
    new PooledDataSources(again).close();
    duration = System.currentTimeMillis() - start;
    assertTrue("Within the retry interval no connection must be attempted, took " + duration + " ms", duration < 100);
    assertEquals(List.of(SCHEMA), again.getSearchPath());
    assertEquals("Within the retry interval the check must not run", retryAfter, DatabaseSettings.getScriptCheckRetryAfter(id));

    DatabaseSettings.resetScriptCheckState();
    assertNull(DatabaseSettings.getScriptCheckRetryAfter(id));
    DatabaseSettings afterReset = settings(id, PG_PW, 1);
    new PooledDataSources(afterReset).close();
    assertEquals(List.of(SCHEMA), afterReset.getSearchPath());
    assertNotNull("After the reset the check must run again", DatabaseSettings.getScriptCheckRetryAfter(id));
  }
}
