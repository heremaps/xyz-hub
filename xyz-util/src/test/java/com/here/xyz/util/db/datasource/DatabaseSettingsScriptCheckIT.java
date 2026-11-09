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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.LockHelper;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class DatabaseSettingsScriptCheckIT {
  private static final String PG_HOST = "localhost";
  private static final String PG_DB = "postgres";
  private static final String PG_USER = "postgres";
  private static final String PG_PW = "password";
  private static final String PREFIX = "it" + randomAlpha(5).toLowerCase();
  private static final String SCHEMA = PREFIX + ".functions";
  private static final String BROKEN_PREFIX = "it" + randomAlpha(5).toLowerCase();
  private static final String BROKEN_NEXT_TO_BLOCKED_PREFIX = "it" + randomAlpha(5).toLowerCase();
  private static final String BLOCKED_PREFIX = "it" + randomAlpha(5).toLowerCase();
  private static final String REUSED_PREFIX = "it" + randomAlpha(5).toLowerCase();
  private static final Set<String> TEMPLATE_SCHEMAS = ConcurrentHashMap.newKeySet();
  private static final Set<String> TEMPLATE_FUNCTIONS_PREFIXES = ConcurrentHashMap.newKeySet();

  @AfterAll
  public static void dropSchemas() throws Exception {
    try (SingleConnectionDataSources dsp = new SingleConnectionDataSources(settings(newId(), PG_PW, 5432, null, null), 5, 20)) {
      List<String> prefixes = new ArrayList<>(List.of(PREFIX, BROKEN_PREFIX, BROKEN_NEXT_TO_BLOCKED_PREFIX, BLOCKED_PREFIX,
          REUSED_PREFIX));
      prefixes.addAll(TEMPLATE_FUNCTIONS_PREFIXES);
      for (String prefix : prefixes)
        for (String schema : listSchemas(dsp, prefix))
          new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
      for (String schema : TEMPLATE_SCHEMAS)
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
    }
  }

  //------------ The empty-space template ------------

  private static String newTemplateSchema() {
    String schema = "lazy_" + randomAlpha(5);
    TEMPLATE_SCHEMAS.add(schema);
    return schema;
  }

  /**
   * Settings whose script check installs the functions the template DDL calls into a script schema of their own, and with the
   * template the connector requests.
   */
  private static DatabaseSettings templateSettings(String id, String password, String schema) {
    String prefix = "it" + schema.substring("lazy_".length());
    TEMPLATE_FUNCTIONS_PREFIXES.add(prefix);
    return settings(id, password, 5432, "/templateFunctions", prefix)
        .withSchema(schema)
        .withEmptySpaceTemplate(true);
  }

  private static boolean templateExists(String schema) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return LazySpaceTables.checkLevels(dsp, schema, List.of()).templatePresent();
    }
  }

  private static void execute(String statement, String schema) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      new SQLQuery(statement).withVariable("schema", schema).write(dsp);
    }
  }

  @Test
  public void templateIsVerifiedByTheScriptCheckAndFoundAgainInANewJvmState() throws Exception {
    String id = newId();
    String schema = newTemplateSchema();
    DatabaseSettings settings = templateSettings(id, PG_PW, schema);
    new PooledDataSources(settings).close();
    assertEquals(TemplateCheck.VERIFIED, settings.getEmptySpaceTemplateCheck());
    assertTrue(settings.isEmptySpaceTemplateVerified());
    assertTrue(templateExists(schema));
    assertNull(DatabaseSettings.getScriptCheckRetryAfter(id));

    DatabaseSettings.resetScriptCheckState();
    assertNull("The reset must clear the template result", settings.getEmptySpaceTemplateCheck());
    DatabaseSettings again = templateSettings(id, PG_PW, schema);
    new PooledDataSources(again).close();
    assertEquals(TemplateCheck.VERIFIED, again.getEmptySpaceTemplateCheck());
  }

  @Test
  public void heldTemplateLockLeadsToARetryThatInstallsTheTemplate() throws Exception {
    String id = newId();
    String schema = newTemplateSchema();
    try (SingleConnectionDataSources lockDsp = newProvider()) {
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(LazySpaceTables.templateLockKey(schema), lockConnection);
      //The install timeout set directly on the settings, which have no clamp
      DatabaseSettings first = templateSettings(id, PG_PW, schema).withScriptInstallTimeoutSeconds(1);
      long start = System.currentTimeMillis();
      new PooledDataSources(first).close();
      long duration = System.currentTimeMillis() - start;
      LockHelper.advisoryUnlock(LazySpaceTables.templateLockKey(schema), lockConnection);

      assertTrue("The check must end after the install timeout of 1 s, took " + duration + " ms", duration < 4_000);
      assertEquals(TemplateCheck.NOT_VERIFIED, first.getEmptySpaceTemplateCheck());
      assertFalse(first.isEmptySpaceTemplateVerified());
      assertNotNull("A template that is not verified must be retried", DatabaseSettings.getScriptCheckRetryAfter(id));
      assertFalse(templateExists(schema));
    }

    DatabaseSettings.expireScriptCheckRetry(id);
    DatabaseSettings second = templateSettings(id, PG_PW, schema);
    new PooledDataSources(second).close();
    assertEquals(TemplateCheck.VERIFIED, second.getEmptySpaceTemplateCheck());
    assertNull("The successful check must end the retries", DatabaseSettings.getScriptCheckRetryAfter(id));
    assertTrue(templateExists(schema));
  }

  @Test
  public void installTimeoutFromTheConnectorParamIsClampedToItsMinimum() throws Exception {
    String id = newId();
    String schema = newTemplateSchema();
    ConnectorParameters connectorParams = ConnectorParameters.fromMap(Map.of("scriptInstallTimeoutSeconds", 1));
    assertEquals(5, connectorParams.getScriptInstallTimeoutSeconds());
    try (SingleConnectionDataSources lockDsp = newProvider()) {
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(LazySpaceTables.templateLockKey(schema), lockConnection);
      //Applied as the connector does it in DatabaseHandler.initialize(Event)
      DatabaseSettings settings = templateSettings(id, PG_PW, schema);
      if (connectorParams.getScriptInstallTimeoutSeconds() > 0)
        settings.withScriptInstallTimeoutSeconds(connectorParams.getScriptInstallTimeoutSeconds());
      long start = System.currentTimeMillis();
      new PooledDataSources(settings).close();
      long duration = System.currentTimeMillis() - start;
      LockHelper.advisoryUnlock(LazySpaceTables.templateLockKey(schema), lockConnection);

      assertTrue("The check must end after the clamped install timeout of 5 s, took " + duration + " ms",
          duration >= 4_000 && duration < 9_000);
      assertEquals(TemplateCheck.NOT_VERIFIED, settings.getEmptySpaceTemplateCheck());
      assertFalse(templateExists(schema));
    }
  }

  @Test
  public void templateFoundMissingAtRuntimeStartsNoCheck() throws Exception {
    String id = newId();
    String schema = newTemplateSchema();
    DatabaseSettings settings = templateSettings(id, PG_PW, schema);
    new PooledDataSources(settings).close();
    assertEquals(TemplateCheck.VERIFIED, settings.getEmptySpaceTemplateCheck());

    settings.markEmptySpaceTemplateMissing();
    assertEquals(TemplateCheck.MISSING_AT_RUNTIME, settings.getEmptySpaceTemplateCheck());
    assertFalse(settings.isEmptySpaceTemplateVerified());

    //Settings with the same ID must use the kept result without connecting, which would fail with the wrong password
    DatabaseSettings later = templateSettings(id, "wrongPassword", schema);
    new PooledDataSources(later).close();
    assertEquals(TemplateCheck.MISSING_AT_RUNTIME, later.getEmptySpaceTemplateCheck());
    assertNull("A check with the wrong password would have delayed the next check", DatabaseSettings.getScriptCheckRetryAfter(id));

    //A template that is not identical stays so
    String otherId = newId();
    String otherSchema = newTemplateSchema();
    execute("CREATE SCHEMA ${schema}", otherSchema);
    execute("CREATE TABLE ${schema}.xyz_empty_space_v1 (id INT)", otherSchema);
    DatabaseSettings notIdentical = templateSettings(otherId, PG_PW, otherSchema);
    new PooledDataSources(notIdentical).close();
    assertEquals(TemplateCheck.NOT_IDENTICAL, notIdentical.getEmptySpaceTemplateCheck());
    assertNull("NOT_IDENTICAL is kept for the JVM", DatabaseSettings.getScriptCheckRetryAfter(otherId));
    notIdentical.markEmptySpaceTemplateMissing();
    assertEquals(TemplateCheck.NOT_IDENTICAL, notIdentical.getEmptySpaceTemplateCheck());
  }

  @Test
  public void settingsWithoutTheTemplateDoNotCheckIt() throws Exception {
    String id = newId();
    String schema = newTemplateSchema();
    DatabaseSettings settings = templateSettings(id, PG_PW, schema).withEmptySpaceTemplate(false);
    new PooledDataSources(settings).close();
    assertNull(settings.getEmptySpaceTemplateCheck());
    assertFalse(settings.isEmptySpaceTemplateVerified());
    assertFalse(templateExists(schema));

    //The same ID with the template must check it, although the scripts of the ID were verified already
    DatabaseSettings withTemplate = templateSettings(id, PG_PW, schema);
    new PooledDataSources(withTemplate).close();
    assertEquals(TemplateCheck.VERIFIED, withTemplate.getEmptySpaceTemplateCheck());
    assertTrue(templateExists(schema));
  }

  private static SingleConnectionDataSources newProvider() {
    return new SingleConnectionDataSources(settings(newId(), PG_PW, 5432, null, null), 5, 20);
  }

  private static String callMyTestFunction(DataSourceProvider dsp) throws SQLException {
    return new SQLQuery("SELECT myTestFunction(#{param})")
        .withNamedParameter("param", "TestUser")
        .run(dsp, rs -> rs.next() ? rs.getString(1) : null);
  }

  private static List<String> listSchemas(DataSourceProvider dsp, String prefix) throws SQLException {
    return new SQLQuery("SELECT nspname FROM pg_namespace WHERE nspname LIKE #{pattern}")
        .withNamedParameter("pattern", prefix + ".%")
        .run(dsp, rs -> {
          List<String> result = new ArrayList<>();
          while (rs.next())
            result.add(rs.getString(1));
          return result;
        });
  }

  private static String newId() {
    return DatabaseSettingsScriptCheckIT.class.getSimpleName() + "_" + randomAlpha(5);
  }

  private static DatabaseSettings settings(String id, String password, int port) {
    return settings(id, password, port, "/sqlSamples", PREFIX);
  }

  private static DatabaseSettings settings(String id, String password, int port, String scriptPath, String scriptPrefix) {
    DatabaseSettings settings = new DatabaseSettings(id)
        .withApplicationName(DatabaseSettingsScriptCheckIT.class.getSimpleName())
        .withHost(PG_HOST)
        .withPort(port)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(password);
    return scriptPath == null ? settings
        : settings.withScriptResourcePaths(List.of(new ScriptResourcePath(scriptPath, scriptPrefix)));
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

  @Test
  public void permanentFailureIsKeptWithoutRetry() throws Exception {
    String id = newId();
    String brokenSchema = BROKEN_PREFIX + ".functions";

    DatabaseSettings first = settings(id, PG_PW, 5432, "/functionsBroken", BROKEN_PREFIX);
    new PooledDataSources(first).close();
    assertEquals("The search path falls back to the unversioned schema", List.of(brokenSchema), first.getSearchPath());
    assertNull("A permanent failure must not be retried", DatabaseSettings.getScriptCheckRetryAfter(id));
    try (SingleConnectionDataSources dsp = new SingleConnectionDataSources(settings(newId(), PG_PW, 5432, null, null), 5, 20)) {
      assertEquals("The broken script must not be installed", List.of(), listSchemas(dsp, BROKEN_PREFIX));
    }

    //Settings with the same ID must use the cached result without connecting, which would fail with the wrong password
    DatabaseSettings second = settings(id, "wrongPassword", 5432, "/functionsBroken", BROKEN_PREFIX);
    new PooledDataSources(second).close();
    assertEquals(first.getSearchPath(), second.getSearchPath());
    assertNull("A check with the wrong password would have delayed the next check", DatabaseSettings.getScriptCheckRetryAfter(id));
  }

  private static DatabaseSettings brokenAndBlockedSettings(String id, String password) {
    return settings(id, password, 5432, null, null)
        .withScriptResourcePaths(List.of(new ScriptResourcePath("/functionsBroken", BROKEN_NEXT_TO_BLOCKED_PREFIX),
            new ScriptResourcePath("/sqlSamples", BLOCKED_PREFIX)))
        .withScriptInstallTimeoutSeconds(1);
  }

  @Test
  public void permanentFailureOfOneScriptDoesNotStopTheRetryOfABlockedScript() throws Exception {
    String id = newId();
    String brokenSchema = BROKEN_NEXT_TO_BLOCKED_PREFIX + ".functions";
    String blockedSchema = BLOCKED_PREFIX + ".functions";
    List<String> searchPath = List.of(brokenSchema, blockedSchema);

    try (SingleConnectionDataSources lockDsp = newProvider()) {
      //Another installer holds the lock on the second script's schema, so its installation fails with a transient error
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(blockedSchema, lockConnection);
      DatabaseSettings first = brokenAndBlockedSettings(id, PG_PW);
      new PooledDataSources(first).close();
      assertEquals(searchPath, first.getSearchPath());
      assertNotNull("The transient failure of the blocked script must be retried", DatabaseSettings.getScriptCheckRetryAfter(id));
      assertEquals("The blocked script must not be installed", List.of(), listSchemas(lockDsp, BLOCKED_PREFIX));

      LockHelper.advisoryUnlock(blockedSchema, lockConnection);
      //Another attempt to install the broken script would now wait for this lock and fail with a transient error
      LockHelper.advisoryLock(brokenSchema, lockConnection);
      DatabaseSettings.expireScriptCheckRetry(id);

      //New settings with the same ID after the retry interval, as the connector creates them for each invocation
      DatabaseSettings second = brokenAndBlockedSettings(id, PG_PW);
      try (PooledDataSources dsp = new PooledDataSources(second)) {
        assertEquals(searchPath, second.getSearchPath());
        assertEquals("The blocked script must be installed and used after the retry", "Hello World, TestUser", callMyTestFunction(dsp));
      }
      assertNull("The retry must not attempt the broken script again, and the remaining permanent failure must not be retried",
          DatabaseSettings.getScriptCheckRetryAfter(id));
      LockHelper.advisoryUnlock(brokenSchema, lockConnection);
      assertEquals("The broken script must not be installed", List.of(), listSchemas(lockDsp, BROKEN_NEXT_TO_BLOCKED_PREFIX));
    }

    //Settings with the same ID must use the cached result without connecting, which would fail with the wrong password
    DatabaseSettings third = brokenAndBlockedSettings(id, "wrongPassword");
    new PooledDataSources(third).close();
    assertEquals(searchPath, third.getSearchPath());
    assertNull("A check with the wrong password would have delayed the next check", DatabaseSettings.getScriptCheckRetryAfter(id));
  }

  @Test
  public void reusedSettingsCheckAgainAfterTheRetryInterval() throws Exception {
    String schema = REUSED_PREFIX + ".functions";
    //The DB holds an older content of the script
    new PooledDataSources(settings(newId(), PG_PW, 5432, "/functions0", REUSED_PREFIX)).close();

    String id = newId();
    //One settings object for all pools, as the job steps keep it while they close and create pools
    DatabaseSettings reused = settings(id, PG_PW, 5432, "/functions2", REUSED_PREFIX).withScriptInstallTimeoutSeconds(1);
    try (SingleConnectionDataSources lockDsp = newProvider()) {
      //Another installer holds the lock on the schema, so the installation of the new content fails with a transient error
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(schema, lockConnection);
      new PooledDataSources(reused).close();
      LockHelper.advisoryUnlock(schema, lockConnection);
    }
    Long retryAfter = DatabaseSettings.getScriptCheckRetryAfter(id);
    assertNotNull("The blocked installation must be retried", retryAfter);
    assertEquals(List.of(schema), reused.getSearchPath());

    //Within the retry interval, a new pool must not check again. A check would now install the new content.
    try (PooledDataSources dsp = new PooledDataSources(reused)) {
      assertEquals("Within the retry interval the older content must be used", "Hello Friend, TestUser", callMyTestFunction(dsp));
    }
    assertEquals("Within the retry interval the check must not run", retryAfter, DatabaseSettings.getScriptCheckRetryAfter(id));

    DatabaseSettings.expireScriptCheckRetry(id);
    try (PooledDataSources dsp = new PooledDataSources(reused)) {
      assertEquals("After the retry interval a new pool must check again and use the new content", "Hello, TestUser",
          callMyTestFunction(dsp));
    }
    assertNull("The successful check must end the retries", DatabaseSettings.getScriptCheckRetryAfter(id));
    assertEquals("The search path must contain the script schema once", List.of(schema), reused.getSearchPath());
  }
}
