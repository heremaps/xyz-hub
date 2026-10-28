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

package com.here.xyz.jobs.steps.execution.db;

import static com.here.xyz.util.Random.randomAlpha;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs the query wrapping of {@link DatabaseBasedStep} asynchronously, as the steps do, but with callbacks that write
 * into a log table instead of invoking the step lambda.
 */
public class DatabaseBasedStepCallbackIT {
  private static final String SCHEMA = "public";
  private static final String RUN_LABEL = "callbackTestRun";
  private static final int ROW_COUNT = 100;
  private static final long WAIT_TIMEOUT_MILLIS = 60_000;
  private static final long POLL_INTERVAL_MILLIS = 100;

  //NOTE: Same connection coordinates as used by StepTestBase (which keeps them private).
  private static final DatabaseSettings DB_SETTINGS = new DatabaseSettings("testDatabaseBasedStepCallback")
      .withApplicationName(DatabaseBasedStepCallbackIT.class.getSimpleName())
      .withHost(System.getProperty("pg.host", "localhost"))
      .withDb("postgres")
      .withUser("postgres")
      .withPassword("password")
      .withDbMaxPoolSize(2)
      .withScriptResourcePaths(List.of(
          new ScriptResourcePath("/sql", "jobs", "common"),
          new ScriptResourcePath("/jobs", "jobs")
      ));

  private final String runId = getClass().getSimpleName() + "_" + randomAlpha(5);
  private final String dataTable = runId + "_data";
  private final String logTable = runId + "_log";
  private DataSourceProvider dsp;

  @BeforeEach
  public void setup() throws SQLException {
    dsp = new PooledDataSources(DB_SETTINGS);
    new SQLQuery("""
        CREATE TABLE ${schema}.${dataTable} (id INT, CONSTRAINT ${uniqueConstraint} UNIQUE (id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TABLE ${schema}.${logTable} (kind TEXT, step_xid XID8, step_status TEXT, visible_rows BIGINT, error_code TEXT);
        """)
        .withVariable("schema", SCHEMA)
        .withVariable("dataTable", dataTable)
        .withVariable("logTable", logTable)
        .withVariable("uniqueConstraint", dataTable + "_unique")
        .write(dsp);
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dsp == null)
      return;
    try {
      new SQLQuery("DROP TABLE IF EXISTS ${schema}.${dataTable}; DROP TABLE IF EXISTS ${schema}.${logTable};")
          .withVariable("schema", SCHEMA)
          .withVariable("dataTable", dataTable)
          .withVariable("logTable", logTable)
          .write(dsp);
    }
    finally {
      dsp.close();
    }
  }

  @Test
  public void successCallbackRunsAfterCommit() throws Exception {
    runWrapped(new SQLQuery("INSERT INTO ${schema}.${dataTable} SELECT generate_series(1, #{rowCount})")
        .withNamedParameter("rowCount", ROW_COUNT));

    waitForLogEntry("success");
    waitTillNotRunning();

    List<LogEntry> log = loadLog();
    assertEquals(List.of("success"), log.stream().map(LogEntry::kind).toList());
    //The success callback must see the transaction of the step query as committed, and all of its data
    assertEquals("committed", log.get(0).stepStatus());
    assertEquals(ROW_COUNT, log.get(0).visibleRows());
  }

  @Test
  public void failingStepQueryOnlyRunsFailureCallback() throws Exception {
    runWrapped(new SQLQuery("INSERT INTO ${schema}.${dataTable} SELECT 1 / 0"));

    assertOnlyFailureCallback("22012");
  }

  @Test
  public void deferredConstraintViolationOnlyRunsFailureCallback() throws Exception {
    //The unique constraint is deferred, so the duplicates are only detected at the end of the transaction
    runWrapped(new SQLQuery("INSERT INTO ${schema}.${dataTable} VALUES (1), (1)"));

    assertOnlyFailureCallback("23505");
  }

  private void assertOnlyFailureCallback(String expectedErrorCode) throws Exception {
    waitForLogEntry("failure");
    waitTillNotRunning();
    //Give a wrongly started success callback the chance to show up
    Thread.sleep(1_000);
    waitTillNotRunning();

    List<LogEntry> log = loadLog();
    assertEquals(List.of("failure"), log.stream().map(LogEntry::kind).toList());
    assertEquals(expectedErrorCode, log.get(0).errorCode());
    assertEquals(0, countDataRows());
  }

  private void runWrapped(SQLQuery stepQuery) throws SQLException {
    //Records the transaction of the step query, so that the success callback can check whether it was committed
    SQLQuery recordingStepQuery = new SQLQuery("""
        ${{stepQuery}};
        INSERT INTO ${schema}.${logTable} (kind, step_xid) VALUES ('step', pg_current_xact_id())
        """)
        .withQueryFragment("stepQuery", stepQuery);

    SQLQuery successCallback = DatabaseBasedStep.deferAfterCommit(new SQLQuery("""
        INSERT INTO ${schema}.${logTable} (kind, step_status, visible_rows)
          SELECT 'success', pg_xact_status(step_xid), (SELECT count(*) FROM ${schema}.${dataTable})
          FROM ${schema}.${logTable} WHERE kind = 'step'
        """));

    SQLQuery failureCallback = new SQLQuery("INSERT INTO ${schema}.${logTable} (kind, error_code) VALUES ('failure', SQLSTATE);");

    DatabaseBasedStep.wrapQuery(recordingStepQuery, successCallback, failureCallback)
        .withVariable("schema", SCHEMA)
        .withVariable("dataTable", dataTable)
        .withVariable("logTable", logTable)
        .withAsync(true)
        .withLabel(RUN_LABEL, runId)
        .run(dsp);
  }

  private void waitForLogEntry(String kind) throws Exception {
    long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
    while (loadLog().stream().noneMatch(entry -> kind.equals(entry.kind()))) {
      assertTrue(System.currentTimeMillis() < deadline, "No " + kind + " entry after " + WAIT_TIMEOUT_MILLIS + "ms");
      Thread.sleep(POLL_INTERVAL_MILLIS);
    }
  }

  private void waitTillNotRunning() throws Exception {
    long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MILLIS;
    while (SQLQuery.isRunning(dsp, false, RUN_LABEL, runId)) {
      assertTrue(System.currentTimeMillis() < deadline, "Queries still running after " + WAIT_TIMEOUT_MILLIS + "ms");
      Thread.sleep(POLL_INTERVAL_MILLIS);
    }
  }

  private List<LogEntry> loadLog() throws SQLException {
    return new SQLQuery("SELECT kind, step_status, visible_rows, error_code FROM ${schema}.${logTable} WHERE kind != 'step'")
        .withVariable("schema", SCHEMA)
        .withVariable("logTable", logTable)
        .run(dsp, rs -> {
          List<LogEntry> entries = new ArrayList<>();
          while (rs.next())
            entries.add(new LogEntry(rs.getString("kind"), rs.getString("step_status"), rs.getLong("visible_rows"),
                rs.getString("error_code")));
          return entries;
        });
  }

  private long countDataRows() throws SQLException {
    return new SQLQuery("SELECT count(*) FROM ${schema}.${dataTable}")
        .withVariable("schema", SCHEMA)
        .withVariable("dataTable", dataTable)
        .run(dsp, rs -> rs.next() ? rs.getLong(1) : -1L);
  }

  private record LogEntry(String kind, String stepStatus, long visibleRows, String errorCode) {}
}
