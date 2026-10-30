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

package com.here.xyz.psql.sql;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.psql.query.branching.QueryTestBase;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The creation of the next history partition in advance (DS-2341) while another session holds an ACCESS SHARE lock on the
 * space table, which blocks the ACCESS EXCLUSIVE lock the creation needs. Covers both write paths: the plpgsql function
 * {@code xyz_write_versioned_modification_operation} of the ModifyFeatures path and the FeatureWriter ({@code write_features}).
 *
 * <p>A write at a trigger version must wait for the lock only once (about 200 ms) instead of once per row, keep its own
 * lock timeout, and must still be canceled by the statement timeout. Unless stated otherwise, the writer runs in a transaction
 * with a lock timeout of 2 s and a statement timeout of 3 s.</p>
 */
public class HistoryPartitionPreCreationIT extends QueryTestBase {

  private static final String VERSIONED_WRITE = "SELECT xyz_write_versioned_modification_operation(?, ?::BIGINT, 'I', ?::JSONB, "
      + "NULL::GEOMETRY, 'public', ?, false, 100000::BIGINT, 1000, NULL::TEXT, NULL::BIGINT, -1::BIGINT)";
  private static final String ATTEMPT_WARNING = "history partition";
  private static final String CREATION_NOTICE = "Creating new history partition";
  //The attempt waits 200 ms for the lock, waiting the 2 s lock timeout of the transaction once would exceed this
  private static final long MAX_BLOCKED_MILLIS = 1_000;
  private static final long MIN_BLOCKED_MILLIS = 150;
  private static final List<String> TABLES = new ArrayList<>();

  private String table;

  @BeforeEach
  void createTable() throws Exception {
    table = getClass().getSimpleName() + "_" + Random.randomAlpha(5);
    TABLES.add(table);
    createSpaceTable(PG_SCHEMA, table);
  }

  @AfterAll
  static void dropTables() throws Exception {
    for (String table : TABLES)
      dropTable(PG_SCHEMA, table);
  }

  @Test
  void halfFullAttemptCreatesNextPartition() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp)) {
      beginWithTimeouts(writer, "2s", "3s");
      Messages messages = writeVersioned(writer, 50_500, 10);
      assertEquals(1, messages.creationNotices(), "The creation must be attempted once per transaction, not per row");
      assertEquals(0, messages.attemptWarnings());
      assertEquals("2s", showLockTimeout(writer), "The lock timeout of the transaction must be restored");
      writer.commit();
      assertTrue(tableExists(writer, table + "_p1"));
    }
  }

  @Test
  void blockedHalfFullAttemptWaitsOnceAndKeepsTheWrite() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp);
        Connection blocker = dsp.getWriter().getConnection()) {
      holdAccessShareLock(blocker, writer);
      try {
        beginWithTimeouts(writer, "2s", "3s");
        Messages messages = writeVersioned(writer, 50_500, 10);
        assertWaitedOnce(messages);
        assertEquals("2s", showLockTimeout(writer), "The lock timeout of the transaction must be restored");
        writer.commit();
      }
      finally {
        blocker.rollback();
      }
      assertEquals(10, countRows(writer, table + "_head"));
      assertFalse(tableExists(writer, table + "_p1"));
    }
  }

  @Test
  void blockedAttemptIsMadeOncePerVersionAcrossAutocommitStatements() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp);
        Connection blocker = dsp.getWriter().getConnection()) {
      holdAccessShareLock(blocker, writer);
      try {
        //Like the non-transactional ModifyFeatures path: every row in its own transaction, with the session's timeouts
        writer.setAutoCommit(true);
        Messages sameVersion = writeVersioned(writer, 51_500, 3);
        assertWaitedOnce(sameVersion);

        Messages nextVersion = writeVersioned(writer, 52_500, 1);
        assertEquals(1, nextVersion.attemptWarnings(), "A new trigger version must be attempted again");
      }
      finally {
        blocker.rollback();
      }
    }
  }

  @Test
  void blockedNearlyFullAttemptKeepsTheWrite() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp);
        Connection blocker = dsp.getWriter().getConnection()) {
      holdAccessShareLock(blocker, writer);
      try {
        beginWithTimeouts(writer, "2s", "3s");
        //The write does not need the next partition yet, so failing to create it must not fail the write
        Messages messages = writeVersioned(writer, 99_960, 5);
        assertWaitedOnce(messages);
        writer.commit();
      }
      finally {
        blocker.rollback();
      }
      assertEquals(5, countRows(writer, table + "_head"));
    }
  }

  @Test
  void statementTimeoutStillCancelsThePlpgsqlWrite() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp);
        Connection blocker = dsp.getWriter().getConnection()) {
      holdAccessShareLock(blocker, writer);
      try {
        beginWithTimeouts(writer, "2s", "100ms");
        long start = System.nanoTime();
        SQLException e = assertThrows(SQLException.class, () -> writeVersioned(writer, 53_500, 1));
        long millis = millisSince(start);
        assertEquals("57014", e.getSQLState(), "Expected the statement timeout, but got: " + e.getMessage());
        assertTrue(millis < MAX_BLOCKED_MILLIS, "The cancellation took " + millis + " ms");
        writer.rollback();
      }
      finally {
        blocker.rollback();
      }
    }
  }

  @Test
  void featureWriterBlockedAttemptWaitsOnce() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp);
        Connection blocker = dsp.getWriter().getConnection()) {
      holdAccessShareLock(blocker, writer);
      try {
        beginWithTimeouts(writer, "2s", "3s");
        Messages messages = writeFeatures(writer, 54_500, 10);
        assertWaitedOnce(messages);
        assertEquals("2s", showLockTimeout(writer), "The lock timeout of the transaction must be restored");
        writer.commit();
      }
      finally {
        blocker.rollback();
      }
      assertEquals(10, countRows(writer, table + "_head"));
      assertFalse(tableExists(writer, table + "_p1"));
    }
  }

  @Test
  void featureWriterCreatesNextPartition() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp)) {
      beginWithTimeouts(writer, "2s", "3s");
      Messages messages = writeFeatures(writer, 55_500, 10);
      assertEquals(1, messages.creationNotices(), "The creation must be attempted once per write, not per feature");
      assertEquals(0, messages.attemptWarnings());
      assertEquals("2s", showLockTimeout(writer), "The lock timeout of the transaction must be restored");
      writer.commit();
      assertTrue(tableExists(writer, table + "_p1"));
    }
  }

  @Test
  void featureWriterStatementTimeoutIsNotSwallowed() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider(); Connection writer = writerConnection(dsp);
        Connection blocker = dsp.getWriter().getConnection()) {
      holdAccessShareLock(blocker, writer);
      try {
        beginWithTimeouts(writer, "2s", "100ms");
        long start = System.nanoTime();
        SQLException e = assertThrows(SQLException.class, () -> writeFeatures(writer, 56_500, 5));
        long millis = millisSince(start);
        //write_features() wraps the error, so its SQL state is not the one of the cancellation
        assertTrue(e.getMessage().contains("canceling statement due to statement timeout"), "Unexpected error: " + e.getMessage());
        assertTrue(millis < MAX_BLOCKED_MILLIS, "The cancellation took " + millis + " ms");
        writer.rollback();
      }
      finally {
        blocker.rollback();
      }
    }
  }

  /**
   * @param attemptWarnings The WARNINGs of failed attempts to create the next history partition
   * @param creationNotices The NOTICEs of xyz_create_history_partition(), one per attempt
   */
  private record Messages(long millis, int attemptWarnings, int creationNotices) {}

  private static void assertWaitedOnce(Messages messages) {
    assertEquals(1, messages.attemptWarnings(), "The creation must be attempted once, not per row");
    assertTrue(messages.millis() >= MIN_BLOCKED_MILLIS && messages.millis() < MAX_BLOCKED_MILLIS,
        "The write took " + messages.millis() + " ms, expected one wait of about 200 ms");
  }

  /**
   * A writer connection that receives the WARNINGs and NOTICEs, which the server configuration may suppress
   */
  private static Connection writerConnection(DataSourceProvider dsp) throws SQLException {
    Connection writer = dsp.getWriter().getConnection();
    try (Statement statement = writer.createStatement()) {
      statement.execute("SET client_min_messages = notice");
    }
    return writer;
  }

  private static void beginWithTimeouts(Connection writer, String lockTimeout, String statementTimeout) throws SQLException {
    writer.setAutoCommit(false);
    try (Statement statement = writer.createStatement()) {
      statement.execute("SET LOCAL lock_timeout = '" + lockTimeout + "'");
      statement.execute("SET LOCAL statement_timeout = '" + statementTimeout + "'");
    }
  }

  /**
   * Opens a transaction on the blocker that reads the space table and keeps its ACCESS SHARE locks until it is rolled back.
   */
  private void holdAccessShareLock(Connection blocker, Connection writer) throws SQLException {
    blocker.setAutoCommit(false);
    try (Statement statement = blocker.createStatement(); ResultSet rs = statement.executeQuery(
        "SELECT count(*) FROM " + PG_SCHEMA + ".\"" + table + "\"")) {
      rs.next();
    }

    try (PreparedStatement statement = writer.prepareStatement("SELECT count(*) FROM pg_locks WHERE relation = to_regclass(?) "
        + "AND mode = 'AccessShareLock' AND granted AND pid <> pg_backend_pid()")) {
      statement.setString(1, qualified(table));
      try (ResultSet rs = statement.executeQuery()) {
        assertTrue(rs.next() && rs.getInt(1) >= 1, "The blocker must hold an ACCESS SHARE lock on the space table");
      }
    }
  }

  /**
   * Writes the specified number of new features at the specified version, each with its own call of the function, as the
   * ModifyFeatures path does.
   */
  private Messages writeVersioned(Connection writer, long version, int rows) throws SQLException {
    int attemptWarnings = 0;
    int creationNotices = 0;
    long start = System.nanoTime();
    try (PreparedStatement statement = writer.prepareStatement(VERSIONED_WRITE)) {
      for (int i = 0; i < rows; i++) {
        String id = "F" + version + "_" + i;
        statement.setString(1, id);
        statement.setLong(2, version);
        statement.setString(3, "{\"type\":\"Feature\",\"id\":\"" + id + "\",\"properties\":{\"@ns:com:here:xyz\":{\"author\":\"test\"}}}");
        statement.setString(4, table);
        statement.execute();
        attemptWarnings += countMessages(statement.getWarnings(), "01000", ATTEMPT_WARNING);
        creationNotices += countMessages(statement.getWarnings(), "00000", CREATION_NOTICE);
        statement.clearWarnings();
      }
    }
    return new Messages(millisSince(start), attemptWarnings, creationNotices);
  }

  /**
   * Writes the specified number of new features at the specified version with one call of write_features().
   */
  private Messages writeFeatures(Connection writer, long version, int features) throws SQLException {
    try (PreparedStatement context = writer.prepareStatement("SELECT context(?::JSONB)");
        PreparedStatement sequence = writer.prepareStatement("SELECT setval(?::REGCLASS, ?)")) {
      context.setString(1, XyzSerializable.serialize(new FeatureWriterQueryContextBuilder()
          .withSchema(PG_SCHEMA)
          .withTable(table)
          .withSpaceContext(DEFAULT)
          .withHistoryEnabled(true)
          .withBatchMode(true)
          .build()));
      context.execute();
      sequence.setString(1, qualified(table + "_version_seq"));
      sequence.setLong(2, version - 1);
      sequence.execute();
    }

    String input = IntStream.range(0, features)
        .mapToObj(i -> "{\"type\":\"Feature\",\"id\":\"a" + version + "_" + i + "\",\"properties\":{\"@ns:com:here:xyz\":{}}}")
        .collect(Collectors.joining(",", "[", "]"));
    long start = System.nanoTime();
    try (PreparedStatement statement = writer.prepareStatement("SELECT write_features(?, 'Features', 'test', false)")) {
      statement.setString(1, input);
      statement.execute();
      return new Messages(millisSince(start), countMessages(statement.getWarnings(), "01000", ATTEMPT_WARNING),
          countMessages(statement.getWarnings(), "00000", CREATION_NOTICE));
    }
  }

  private static int countMessages(SQLWarning warning, String sqlState, String messagePart) {
    int count = 0;
    for (; warning != null; warning = warning.getNextWarning())
      if (sqlState.equals(warning.getSQLState()) && warning.getMessage() != null && warning.getMessage().contains(messagePart))
        count++;
    return count;
  }

  private static String showLockTimeout(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SHOW lock_timeout")) {
      rs.next();
      return rs.getString(1);
    }
  }

  private static boolean tableExists(Connection connection, String table) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      statement.setString(1, qualified(table));
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  private static long countRows(Connection connection, String table) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + qualified(table))) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static String qualified(String table) {
    return PG_SCHEMA + ".\"" + table + "\"";
  }

  private static long millisSince(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }
}
