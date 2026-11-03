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
import static com.here.xyz.util.db.pg.ScriptInstaller.State.PERMANENT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.TRANSIENT;
import static com.here.xyz.util.db.pg.ScriptInstaller.State.VERIFIED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;

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
import java.util.function.IntSupplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ScriptInstallerIT extends SQLITBase {
  private static final Set<String> PREFIXES = ConcurrentHashMap.newKeySet();
  private static final int CHECK_TIMEOUT_SECONDS = 5;
  private static final int SOCKET_TIMEOUT_SECONDS = 20;
  private static final int INSTALL_TIMEOUT_SECONDS = 15;
  private static final int VERSIONS_TO_KEEP = 5;
  private static final String FUNCTIONS_0 = "/functions0/functions.sql";
  private static final String FUNCTIONS_2 = "/functions2/functions.sql";
  private static final String FUNCTIONS_BROKEN = "/functionsBroken/functions.sql";
  private static final String SQL_SAMPLES = "/sqlSamples/functions.sql";
  private static final List<String> CAPTURED_LOGGERS = List.of(Script.class.getName(), ScriptInstaller.class.getName());
  private static final List<Level> PREVIOUS_LEVELS = new ArrayList<>();

  @BeforeAll
  public static void enableWarnings() {
    //The tests count the WARNs of the installer, which the default configuration does not log
    for (String logger : CAPTURED_LOGGERS) {
      PREVIOUS_LEVELS.add(LogManager.getLogger(logger).getLevel());
      Configurator.setLevel(logger, Level.WARN);
    }
  }

  @AfterAll
  public static void dropSchemas() throws Exception {
    try {
      for (String prefix : PREFIXES)
        try (SingleConnectionDataSources dsp = newProvider()) {
          for (String schema : listSchemas(prefix))
            new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
        }
    }
    finally {
      for (int i = 0; i < PREVIOUS_LEVELS.size(); i++)
        Configurator.setLevel(CAPTURED_LOGGERS.get(i), PREVIOUS_LEVELS.get(i));
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
    return checkAndInstall(dsp, resource, version, prefix, installTimeoutSeconds, versionsToKeep, null);
  }

  private static Outcome checkAndInstall(DataSourceProvider dsp, String resource, String version, String prefix, int installTimeoutSeconds,
      int versionsToKeep, IntSupplier remainingMillis) {
    return ScriptInstaller.checkAndInstall(List.of(new Script(resource, dsp, version, prefix)), dsp, version, versionsToKeep,
        installTimeoutSeconds, remainingMillis);
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

  private static long schemaOid(String schema) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return new SQLQuery("SELECT oid FROM pg_namespace WHERE nspname = #{schema}")
          .withNamedParameter("schema", schema)
          .run(dsp, rs -> rs.next() ? rs.getLong(1) : -1L);
    }
  }

  /**
   * @return The tables, materialized views, views and composite types of both schemas with their kind and columns, sorted
   */
  private static List<String> listRelations(String schema, String otherSchema) throws SQLException {
    try (SingleConnectionDataSources dsp = newProvider()) {
      return new SQLQuery("""
          SELECT n.nspname || '.' || c.relname || ' ' || c.relkind::text || ' (' || coalesce((SELECT string_agg(a.attname || ' '
              || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum) FROM pg_attribute a
              WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped), '') || ')'
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN (#{schema}, #{otherSchema}) AND c.relkind IN ('r', 'm', 'v', 'c')
          """)
          .withNamedParameter("schema", schema)
          .withNamedParameter("otherSchema", otherSchema)
          .run(dsp, rs -> {
            List<String> relations = new ArrayList<>();
            while (rs.next())
              relations.add(rs.getString(1));
            return relations.stream().sorted().toList();
          });
    }
  }

  private static CapturingAppender startCapturing() {
    CapturingAppender appender = new CapturingAppender();
    appender.start();
    CAPTURED_LOGGERS.forEach(logger -> ((Logger) LogManager.getLogger(logger)).addAppender(appender));
    return appender;
  }

  private static void stopCapturing(CapturingAppender appender) {
    CAPTURED_LOGGERS.forEach(logger -> ((Logger) LogManager.getLogger(logger)).removeAppender(appender));
    appender.stop();
  }

  private static long countInstallations(List<String> messages, String schema) {
    return messages.stream()
        .filter(message -> message.startsWith("Installed script ") && message.contains(" into schema " + schema + " in "))
        .count();
  }

  @Test
  public void installOnceThenCheckWithOneQueryOnOneConnection() throws Exception {
    String prefix = newPrefix();

    try (SingleConnectionDataSources dsp = newProvider()) {
      Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP);
      assertEquals(VERIFIED, outcome.state());
      assertEquals(List.of(prefix + ".functions:1.0.1"), outcome.schemas());
      assertEquals("Check, timeouts, versioned schema, unversioned schema, timeout reset, re-check", 6, outcome.statements());
      assertEquals(1, dsp.getOpenedConnectionCount());
      //The re-check after the installation must run with the check's timeout, not with the one of the install statements
      assertEquals(CHECK_TIMEOUT_SECONDS + "s", new SQLQuery("SHOW statement_timeout").run(dsp, rs -> rs.next() ? rs.getString(1) : null));
      assertEquals(Boolean.TRUE, new SQLQuery("""
          SELECT bool_and(setting = reset_val) FROM pg_settings WHERE name IN ('statement_timeout', 'lock_timeout')
          """).run(dsp, rs -> rs.next() ? rs.getBoolean(1) : null));
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
    CapturingAppender appender = startCapturing();
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
      stopCapturing(appender);
    }

    //Without a lock that is held until the commit, a waiting installer would run its DDL before it can see the winner's schema
    //and fail, e.g., with a duplicate key on CREATE SCHEMA, or install the same schema once more
    List<String> warnings = appender.messages(Level.WARN, prefix);
    assertEquals("No installer must fail", List.of(),
        warnings.stream().filter(message -> message.startsWith("Unable to install")).toList());
    assertEquals("Exactly one installer must install the versioned schema", 1, countInstallations(warnings, prefix + ".functions:1.0.1"));
    assertEquals("Exactly one installer must rewrite the unversioned schema", 1, countInstallations(warnings, prefix + ".functions"));
    assertEquals("Only the two schemas must be installed", 2,
        warnings.stream().filter(message -> message.startsWith("Installed script ")).count());

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
  public void installStatementsLeaveTheReservedTimeForTheRequest() throws Exception {
    String prefix = newPrefix();
    String targetSchema = prefix + ".functions:1.0.1";

    try (SingleConnectionDataSources lockDsp = newProvider()) {
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(targetSchema, lockConnection);

      long start = System.currentTimeMillis();
      try (SingleConnectionDataSources dsp = newProvider()) {
        //With 15 s remaining, the install statement may only take 15 s - 10 s = 5 s instead of the install timeout of 15 s
        Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP, () -> 15_000);
        assertEquals(TRANSIENT, outcome.state());
        assertEquals(List.of(prefix + ".functions"), outcome.schemas());
      }
      long duration = System.currentTimeMillis() - start;
      assertTrue("The install statement must give up after the remaining time minus the reserved time, took " + duration + " ms",
          duration >= 4_500 && duration < 10_000);
      assertEquals(List.of(), listSchemas(prefix));

      LockHelper.advisoryUnlock(targetSchema, lockConnection);
    }
  }

  @Test
  public void tooLittleRemainingTimeLeadsToTransientStateWithoutInstallation() throws Exception {
    String prefix = newPrefix();

    try (SingleConnectionDataSources dsp = newProvider()) {
      Outcome outcome = checkAndInstall(dsp, SQL_SAMPLES, "1.0.1", prefix, INSTALL_TIMEOUT_SECONDS, VERSIONS_TO_KEEP, () -> 14_000);
      assertEquals(TRANSIENT, outcome.state());
      assertEquals("No install statement must be started with less than 15 s remaining", 1, outcome.statements());
      assertEquals(List.of(prefix + ".functions"), outcome.schemas());
    }
    assertEquals(List.of(), listSchemas(prefix));
  }

  @Test
  public void permanentErrorFallsBackToTheNewestInstalledVersion() throws Exception {
    String prefix = newPrefix();
    assertEquals(VERIFIED, checkAndInstall(FUNCTIONS_2, "1.0.0", prefix).state());

    Outcome outcome = checkAndInstall(FUNCTIONS_BROKEN, "1.0.1", prefix);

    assertEquals(PERMANENT, outcome.state());
    assertEquals(List.of(prefix + ".functions:1.0.0"), outcome.schemas());
    assertEquals(List.of(prefix + ".functions", prefix + ".functions:1.0.0"), listSchemas(prefix));
    assertEquals("The unversioned schema must not be rewritten after the versioned one failed", "1.0.0",
        outcome.snapshot().getUnversionedVersion(prefix + ".functions"));
    assertEquals("Hello, TestUser", callFunction(prefix + ".functions:1.0.0"));
  }

  @Test
  public void permanentErrorOnASchemaThatIsNoTargetDoesNotMakeATransientMissPermanent() throws Exception {
    String rewriteFailsPrefix = newPrefix();
    String lockedPrefix = newPrefix();
    String unversionedSchema = rewriteFailsPrefix + ".functions";
    String lockedSchema = lockedPrefix + ".functions:1.0.1";
    try (SingleConnectionDataSources dsp = newProvider()) {
      //A procedure with the signature of the script's function lets every rewrite of the unversioned schema fail permanently
      new SQLQuery("CREATE SCHEMA ${schema}").withVariable("schema", unversionedSchema).write(dsp);
      new SQLQuery("CREATE PROCEDURE ${schema}.myTestFunction(input text) LANGUAGE sql AS 'SELECT 1'")
          .withVariable("schema", unversionedSchema)
          .write(dsp);
    }

    CapturingAppender appender = startCapturing();
    Outcome outcome;
    try (SingleConnectionDataSources lockDsp = newProvider()) {
      Connection lockConnection = lockDsp.getWriter().getConnection();
      LockHelper.advisoryLock(lockedSchema, lockConnection);
      try (SingleConnectionDataSources dsp = newProvider()) {
        List<Script> scripts = List.of(new Script(FUNCTIONS_2, dsp, "1.0.1", rewriteFailsPrefix),
            new Script(SQL_SAMPLES, dsp, "1.0.1", lockedPrefix));
        outcome = ScriptInstaller.checkAndInstall(scripts, dsp, "1.0.1", VERSIONS_TO_KEEP, 1, null);
      }
      LockHelper.advisoryUnlock(lockedSchema, lockConnection);
    }
    finally {
      stopCapturing(appender);
    }

    assertEquals("The rewrite of the unversioned schema must have failed with a permanent error", 1,
        appender.messages(Level.WARN, " into schema " + unversionedSchema + " on DB ").stream()
            .filter(message -> message.startsWith("Unable to install") && message.contains("(permanent error)"))
            .count());
    assertEquals("The locked schema must have failed with a transient error", 1,
        appender.messages(Level.WARN, " into schema " + lockedSchema + " on DB ").stream()
            .filter(message -> message.startsWith("Unable to install") && message.contains("(transient error)"))
            .count());
    assertEquals("Only the failed installation of a missing target may be permanent", TRANSIENT, outcome.state());
    assertEquals(List.of(rewriteFailsPrefix + ".functions:1.0.1", lockedPrefix + ".functions"), outcome.schemas());
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

    CapturingAppender appender = startCapturing();
    Outcome outcome;
    try {
      outcome = checkAndInstall(FUNCTIONS_2, "1.0.7", prefix);
    }
    finally {
      stopCapturing(appender);
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
    assertEquals("No ERROR must be logged for the version in use", List.of(), appender.messages(Level.ERROR, prefix));
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
    assertEquals("Check, timeouts, unversioned schema, timeout reset, re-check", 5, outcome.statements());
    assertEquals(List.of(prefix + ".functions"), listSchemas(prefix));
    assertEquals("Hello, TestUser", callFunction(prefix + ".functions"));
    assertEquals(1, countFunctions(prefix + ".functions", "mytestfunction"));
  }

  @Test
  public void reinstallKeepsTheUnversionedSchemaAndObjectsTheScriptDidNotCreate() throws Exception {
    String prefix = newPrefix();
    String schema = prefix + ".functions";
    //Not covered by the prefix cleanup, which only drops the schemas "<prefix>.*"
    String outsideSchema = prefix + "_outside";
    try {
      assertEquals(VERIFIED, checkAndInstall(FUNCTIONS_0, null, prefix).state());
      long schemaOid = schemaOid(schema);

      //Objects in the script schema which the script did not create, and objects outside of it which depend on them
      try (SingleConnectionDataSources dsp = newProvider()) {
        for (String statement : List.of(
            "CREATE TABLE ${schema}.foreign_table (id int)",
            "INSERT INTO ${schema}.foreign_table VALUES (1)",
            "CREATE MATERIALIZED VIEW ${schema}.foreign_mv AS SELECT id FROM ${schema}.foreign_table",
            "CREATE TYPE ${schema}.foreign_type AS (a int)",
            "CREATE SCHEMA ${outsideSchema}",
            "CREATE TABLE ${outsideSchema}.dependent_table (v ${schema}.foreign_type, w ${schema}.foreign_type[])",
            "INSERT INTO ${outsideSchema}.dependent_table VALUES (ROW(1), ARRAY[ROW(2)::${schema}.foreign_type])",
            "CREATE VIEW ${outsideSchema}.dependent_view AS SELECT id FROM ${schema}.foreign_table"))
          new SQLQuery(statement).withVariable("schema", schema).withVariable("outsideSchema", outsideSchema).write(dsp);
      }
      List<String> relations = listRelations(schema, outsideSchema);
      String quotedSchema = "\"" + schema + "\"";
      assertEquals(List.of(
          schema + ".foreign_mv m (id integer)",
          schema + ".foreign_table r (id integer)",
          schema + ".foreign_type c (a integer)",
          outsideSchema + ".dependent_table r (v " + quotedSchema + ".foreign_type, w " + quotedSchema + ".foreign_type[])",
          outsideSchema + ".dependent_view v (id integer)"), relations);

      //Changed content with the same script ID, so the unversioned schema gets reinstalled
      assertEquals(VERIFIED, checkAndInstall(FUNCTIONS_2, null, prefix).state());

      //All checks run, so a failure reports everything the reinstallation dropped
      assertAll(
          () -> assertEquals("The new content must be installed", "Hello, TestUser", callFunction(schema)),
          () -> assertEquals(1, countFunctions(schema, "mytestfunction")),
          () -> assertEquals("The unversioned schema must not be dropped", schemaOid, schemaOid(schema)),
          () -> assertEquals("No object in or outside the schema must be dropped or lose a column", relations,
              listRelations(schema, outsideSchema)),
          () -> {
            try (SingleConnectionDataSources dsp = newProvider()) {
              assertEquals("The rows in and outside the schema must be kept", "1 1 1 1", new SQLQuery("""
                  SELECT (SELECT count(*) FROM ${schema}.foreign_table) || ' ' || (SELECT count(*) FROM ${schema}.foreign_mv) || ' '
                      || (SELECT count(v) || ' ' || count(w) FROM ${outsideSchema}.dependent_table)
                  """)
                  .withVariable("schema", schema)
                  .withVariable("outsideSchema", outsideSchema)
                  .run(dsp, rs -> rs.next() ? rs.getString(1) : null));
            }
          });
    }
    finally {
      try (SingleConnectionDataSources dsp = newProvider()) {
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", outsideSchema).write(dsp);
      }
    }
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

    /**
     * @return The messages of the captured events of the provided level or a more severe one, which contain the provided text
     */
    List<String> messages(Level minimumLevel, String containing) {
      return events.stream()
          .filter(event -> event.getLevel().isMoreSpecificThan(minimumLevel))
          .map(event -> event.getMessage().getFormattedMessage())
          .filter(message -> message.contains(containing))
          .toList();
    }
  }
}
