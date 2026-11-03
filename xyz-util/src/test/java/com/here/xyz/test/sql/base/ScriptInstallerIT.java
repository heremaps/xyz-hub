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

import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.TRANSIENT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.VERIFIED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.SingleConnectionDataSources;
import com.here.xyz.util.db.pg.InstalledScripts;
import com.here.xyz.util.db.pg.LockHelper;
import com.here.xyz.util.db.pg.Script;
import com.here.xyz.util.db.pg.ScriptInstaller;
import com.here.xyz.util.db.pg.ScriptInstaller.Outcome;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class ScriptInstallerIT extends SQLITBase {
  private static final Set<String> PREFIXES = ConcurrentHashMap.newKeySet();
  private static final int CHECK_TIMEOUT_SECONDS = 5;
  private static final int SOCKET_TIMEOUT_SECONDS = 20;
  private static final int INSTALL_TIMEOUT_SECONDS = 15;
  private static final int VERSIONS_TO_KEEP = 5;
  private static final String FUNCTIONS_0 = "/functions0/functions.sql";
  private static final String FUNCTIONS_2 = "/functions2/functions.sql";
  private static final String SQL_SAMPLES = "/sqlSamples/functions.sql";

  @AfterAll
  public static void dropSchemas() throws Exception {
    for (String prefix : PREFIXES)
      try (SingleConnectionDataSources dsp = newProvider()) {
        for (String schema : listSchemas(prefix))
          new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
      }
  }

  private static String newPrefix() {
    String prefix = "it" + randomAlpha(5).toLowerCase();
    PREFIXES.add(prefix);
    return prefix;
  }

  private static DatabaseSettings settings(int port) {
    return new DatabaseSettings(ScriptInstallerIT.class.getSimpleName() + "_" + randomAlpha(5))
        .withApplicationName(ScriptInstallerIT.class.getSimpleName())
        .withHost(PG_HOST)
        .withPort(port)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(PG_PW);
  }

  private static SingleConnectionDataSources newProvider() {
    return new SingleConnectionDataSources(settings(5432), CHECK_TIMEOUT_SECONDS, SOCKET_TIMEOUT_SECONDS);
  }

  private static Outcome checkAndInstall(DataSourceProvider dsp, String resource, String version, String prefix, int installTimeoutSeconds,
      int versionsToKeep) {
    return ScriptInstaller.checkAndInstall(List.of(new Script(resource, dsp, version, prefix)), dsp, version, versionsToKeep,
        installTimeoutSeconds, null);
  }

  private static Outcome checkAndInstall(String resource, String version, String prefix) {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return checkAndInstall(dsp, resource, version, prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP);
    }
  }

  private static List<String> listSchemas(String prefix) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return new SQLQuery("SELECT nspname FROM pg_namespace WHERE nspname LIKE #{pattern} ORDER BY nspname")
          .withNamedParameter("pattern", prefix + ".%")
          .run(dsp, rs -> {
            List<String> schemas = new ArrayList<>();
            while (rs.next())
              schemas.add(rs.getString(1));
            return schemas;
          });
    }
  }

  private static String callFunction(String schema) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return new SQLQuery("SELECT ${schema}.myTestFunction(#{param})")
          .withVariable("schema", schema)
          .withNamedParameter("param", "TestUser")
          .run(dsp, rs -> rs.next() ? rs.getString(1) : null);
    }
  }

  private static long functionOid(String schema) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return new SQLQuery("""
          SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = #{schema} AND p.proname = 'mytestfunction'
          """)
          .withNamedParameter("schema", schema)
          .run(dsp, rs -> rs.next() ? rs.getLong(1) : -1L);
    }
  }

  private static int countFunctions(String schema, String functionName) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return new SQLQuery("""
          SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = #{schema} AND p.proname = #{functionName}
          """)
          .withNamedParameter("schema", schema)
          .withNamedParameter("functionName", functionName)
          .run(dsp, rs -> rs.next() ? rs.getInt(1) : -1);
    }
  }

  @Test
  public void installOnceThenCheckWithOneQueryOnOneConnection() throws Exception {
    String prefix = newPrefix();

    try (SingleConnectionDataSources dsp = newProvider()) {
      Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP);
      assertEquals(VERIFIED, outcome.state());
      assertEquals(List.of(prefix + ".functions:1.0.1"), outcome.schemas());
      assertEquals("Check, timeouts, versioned schema, unversioned schema, re-check", 5, outcome.statements());
      assertEquals(1, dsp.getOpenedConnectionCount());
    }
    assertEquals(List.of(prefix + ".functions", prefix + ".functions:1.0.1"), listSchemas(prefix));
    assertEquals("Hello World, TestUser", callFunction(prefix + ".functions:1.0.1"));
    assertEquals("Hello World, TestUser", callFunction(prefix + ".functions"));

    try (SingleConnectionDataSources dsp = newProvider()) {
      Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP);
      assertEquals(VERIFIED, outcome.state());
      assertEquals(List.of(prefix + ".functions:1.0.1"), outcome.schemas());
      assertEquals("The check of an installed version must be one query", 1, outcome.statements());
      assertEquals("The check of an installed version must use one connection", 1, dsp.getOpenedConnectionCount());

      assertFalse("An installation of an installed schema must be skipped under the lock",
          new Script(SQL_SAMPLES, dsp, "1.0.1", prefix).installSchema(true));
    }
  }

  @Test
  public void versionBumpWithoutContentChangeInstallsOnlyTheMissingVersion() throws Exception {
    String prefix = newPrefix();
    assertEquals(VERIFIED, checkAndInstall(FUNCTIONS_2, "1.0.0", prefix).state());
    long previousVersionOid = functionOid(prefix + ".functions:1.0.0");

    Outcome outcome = checkAndInstall(FUNCTIONS_2, "1.0.1", prefix);

    assertEquals(VERIFIED, outcome.state());
    assertEquals(List.of(prefix + ".functions:1.0.1"), outcome.schemas());
    assertEquals(List.of("1.0.0", "1.0.1"), outcome.snapshot().getVersions(prefix + ".functions"));
    assertEquals("The unversioned schema must report the new version", "1.0.1",
        outcome.snapshot().getUnversionedVersion(prefix + ".functions"));
    assertEquals("The schema of the previous version must not be touched", previousVersionOid, functionOid(prefix + ".functions:1.0.0"));
    assertEquals("Hello, TestUser", callFunction(prefix + ".functions:1.0.1"));
  }

  @Test
  public void concurrentInstallersInstallOnce() throws Exception {
    String prefix = newPrefix();
    assertEquals(VERIFIED, checkAndInstall(FUNCTIONS_0, "1.0.0", prefix).state());

    int installers = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(installers);
    try {
      List<Future<Outcome>> outcomes = new ArrayList<>();
      for (int i = 0; i < installers; i++)
        outcomes.add(executor.submit(() -> {
          start.await();
          return checkAndInstall(FUNCTIONS_2, "1.0.1", prefix);
        }));
      start.countDown();

      for (Future<Outcome> outcome : outcomes)
        assertEquals(VERIFIED, outcome.get(60, TimeUnit.SECONDS).state());
    }
    finally {
      executor.shutdownNow();
    }

    Script script = new Script(FUNCTIONS_2, null, "1.0.1", prefix);
    try (SingleConnectionDataSources dsp = newProvider()) {
      InstalledScripts snapshot = InstalledScripts.load(dsp, List.of(script));
      assertEquals(script.getContentHash(), snapshot.getHash(script.getScriptId(), "1.0.1"));
      assertEquals(script.getContentHash(), snapshot.getHash(script.getScriptId(), null));
      assertEquals("1.0.1", snapshot.getUnversionedVersion(script.getScriptId()));
    }
    assertEquals(1, countFunctions(prefix + ".functions:1.0.1", "script_hash"));
    assertEquals(1, countFunctions(prefix + ".functions", "script_hash"));
    assertEquals(1, countFunctions(prefix + ".functions", "mytestfunction"));
    assertEquals("Hello, TestUser", callFunction(prefix + ".functions:1.0.1"));
  }

  @Test
  public void sessionLockOfAnotherInstallerLeadsToTransientState() throws Exception {
    String prefix = newPrefix();
    String targetSchema = prefix + ".functions:1.0.1";

    try (SingleConnectionDataSources lockDsp = newProvider()) {
      //Hold the lock on the target schema in the way an installer of an older software version holds it
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(targetSchema, lockConnection);

      long start = System.currentTimeMillis();
      try (SingleConnectionDataSources dsp = newProvider()) {
        Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, 1, VERSIONS_TO_KEEP);
        assertEquals(TRANSIENT, outcome.state());
        assertEquals("Without the versioned schema the search path falls back to the unversioned one", List.of(prefix + ".functions"),
            outcome.schemas());
      }
      assertTrue("The install statement must give up after its lock timeout", System.currentTimeMillis() - start < 5_000);
      assertEquals("Nothing must be installed while another installer holds the lock", List.of(), listSchemas(prefix));

      LockHelper.advisoryUnlock(targetSchema, lockConnection);
    }

    assertEquals(VERIFIED, checkAndInstall(SQL_SAMPLES, "1.0.1", prefix).state());
    assertEquals(List.of(prefix + ".functions", targetSchema), listSchemas(prefix));
  }

  @Test
  public void unreachableDbLeadsToTransientStateWithoutInstallation() throws Exception {
    String prefix = newPrefix();

    long start = System.currentTimeMillis();
    try (SingleConnectionDataSources dsp = new SingleConnectionDataSources(settings(1), CHECK_TIMEOUT_SECONDS,
        SOCKET_TIMEOUT_SECONDS)) {
      Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP);
      long duration = System.currentTimeMillis() - start;

      assertEquals(TRANSIENT, outcome.state());
      assertTrue("The check must end within the connect timeout, took " + duration + " ms",
          duration < (CHECK_TIMEOUT_SECONDS + 2) * 1_000L);
      assertEquals(List.of(prefix + ".functions"), outcome.schemas());
      assertNull(outcome.snapshot());
      assertEquals(0, dsp.getOpenedConnectionCount());
    }
    assertEquals(List.of(), listSchemas(prefix));
  }

  @Test
  public void cleanupKeepsTheNewestVersionsAndTheOneInUse() throws Exception {
    String prefix = newPrefix();
    String scriptId = prefix + ".functions";
    for (int patch = 1; patch <= 7; patch++)
      try (SingleConnectionDataSources dsp = newProvider()) {
        assertEquals(VERIFIED, checkAndInstall(dsp, FUNCTIONS_2, "1.0." + patch, prefix, INSTALL_TIMEOUT_SECONDS, 100).state());
      }
    //Let the unversioned schema report the oldest version, as it does after an installer of an older software version rewrote it
    try (SingleConnectionDataSources dsp = newProvider()) {
      assertTrue(new Script(FUNCTIONS_2, dsp, "1.0.1", prefix).installSchema(false));
    }

    CapturingAppender appender = new CapturingAppender();
    List<Logger> loggers = List.of((Logger) LogManager.getLogger(Script.class), (Logger) LogManager.getLogger(ScriptInstaller.class));
    appender.start();
    loggers.forEach(logger -> logger.addAppender(appender));
    Outcome outcome;
    try {
      outcome = checkAndInstall(FUNCTIONS_2, "1.0.7", prefix);
    }
    finally {
      loggers.forEach(logger -> logger.removeAppender(appender));
      appender.stop();
    }

    assertEquals(VERIFIED, outcome.state());
    assertEquals(List.of(prefix + ".functions:1.0.7"), outcome.schemas());
    List<String> keptVersions = List.of("1.0.1", "1.0.3", "1.0.4", "1.0.5", "1.0.6", "1.0.7");
    assertEquals(keptVersions, outcome.snapshot().getVersions(scriptId));
    List<String> expectedSchemas = new ArrayList<>(List.of(scriptId));
    keptVersions.forEach(version -> expectedSchemas.add(scriptId + ":" + version));
    assertEquals(expectedSchemas, listSchemas(prefix));
    try (SingleConnectionDataSources dsp = newProvider()) {
      assertFalse("An uninstallation of a missing schema must be skipped under the lock",
          new Script(FUNCTIONS_2, dsp, "1.0.7", prefix).uninstallVersion("1.0.2"));
    }
    assertEquals("No ERROR must be logged for the version in use", List.of(),
        appender.events.stream()
            .filter(event -> event.getLevel().isMoreSpecificThan(Level.ERROR))
            .map(event -> event.getMessage().getFormattedMessage())
            .filter(message -> message.contains(prefix))
            .toList());
  }

  @Test
  public void withoutVersionOnlyTheUnversionedSchemaIsInstalledAndUpdated() throws Exception {
    String prefix = newPrefix();

    Outcome outcome = checkAndInstall(FUNCTIONS_0, null, prefix);
    assertEquals(VERIFIED, outcome.state());
    assertEquals(List.of(prefix + ".functions"), outcome.schemas());
    assertEquals(List.of(prefix + ".functions"), listSchemas(prefix));
    assertEquals("Hello Friend, TestUser", callFunction(prefix + ".functions"));

    assertEquals("An unchanged content must not be installed again", 1, checkAndInstall(FUNCTIONS_0, null, prefix).statements());

    outcome = checkAndInstall(FUNCTIONS_2, null, prefix);
    assertEquals(VERIFIED, outcome.state());
    assertEquals(List.of(prefix + ".functions"), outcome.schemas());
    assertEquals("Check, timeouts, unversioned schema, re-check", 4, outcome.statements());
    assertEquals(List.of(prefix + ".functions"), listSchemas(prefix));
    assertEquals("Hello, TestUser", callFunction(prefix + ".functions"));
    assertEquals(1, countFunctions(prefix + ".functions", "mytestfunction"));
  }

  private static class CapturingAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    CapturingAppender() {
      super(ScriptInstallerIT.class.getSimpleName() + "-" + randomAlpha(5), null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }
}
