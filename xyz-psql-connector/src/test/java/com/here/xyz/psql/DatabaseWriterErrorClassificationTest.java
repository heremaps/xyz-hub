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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers how {@link DatabaseWriter} classifies the errors of a write: which ones abort it so that {@link DatabaseHandler} can
 * recover a missing history partition or a missing table, and which ones only fail the affected features.
 */
class DatabaseWriterErrorClassificationTest {

  private static SQLException missingPartition() {
    return new SQLException("ERROR: no partition of relation \"t\" found for row", "23514");
  }

  /**
   * The shape pgjdbc gives a failed batch: the server error is only reachable through {@link SQLException#getNextException()}.
   */
  private static BatchUpdateException batchFailedWith(SQLException serverError) {
    BatchUpdateException e = new BatchUpdateException("Batch entry 0 was aborted.", null, 0, new int[0]);
    e.setNextException(serverError);
    return e;
  }

  @Test
  void detectsMissingPartition() {
    SQLException e = missingPartition();
    assertSame(e, DatabaseWriter.findMissingHistoryPartitionException(e));
    assertTrue(DatabaseWriter.isMissingHistoryPartitionException(e));
  }

  @Test
  void ignoresOtherCheckViolations() {
    assertFalse(DatabaseWriter.isMissingHistoryPartitionException(
        new SQLException("ERROR: new row for relation \"t\" violates check constraint \"c\"", "23514")));
  }

  @Test
  void ignoresMissingInformation() {
    assertFalse(DatabaseWriter.isMissingHistoryPartitionException(null));
    assertFalse(DatabaseWriter.isMissingHistoryPartitionException(new SQLException("no partition of relation \"t\"", (String) null)));
    assertFalse(DatabaseWriter.isMissingHistoryPartitionException(new SQLException(null, "23514")));
  }

  @Test
  void detectsMissingPartitionInNextExceptionOfBatch() {
    SQLException serverError = missingPartition();
    BatchUpdateException e = batchFailedWith(serverError);
    assertSame(serverError, DatabaseWriter.findMissingHistoryPartitionException(e));
  }

  @Test
  void detectsMissingPartitionInCauses() {
    SQLException serverError = missingPartition();
    assertSame(serverError, DatabaseWriter.findMissingHistoryPartitionException(new RuntimeException("wrapper", serverError)));
    assertSame(serverError, DatabaseWriter.findMissingHistoryPartitionException(new SQLException("wrapper", "XX000", serverError)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void nonTransactionalWriteAbortsOnMissingTable(boolean partitionRecoverable) {
    SQLException e = new SQLException("ERROR: relation \"t\" does not exist", "42P01");
    assertSame(e, DatabaseWriter.findNonTransactionalAbortCause(e, partitionRecoverable));
  }

  @Test
  void nonTransactionalWriteAbortsOnMissingPartitionOnlyWhileRecoverable() {
    SQLException e = missingPartition();
    assertSame(e, DatabaseWriter.findNonTransactionalAbortCause(e, true));
    assertNull(DatabaseWriter.findNonTransactionalAbortCause(e, false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"55P03", "XYZ49", "XYZ50"})
  void nonTransactionalWriteOnlyFailsTheFeatureOnOtherErrors(String sqlState) {
    SQLException e = new SQLException("ERROR: some failure", sqlState);
    assertNull(DatabaseWriter.findNonTransactionalAbortCause(e, true));
    assertNull(DatabaseWriter.findNonTransactionalAbortCause(e, false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"42P01", "55P03"})
  void batchIsRethrownOnMissingTableAndLockTimeout(String sqlState) {
    SQLException e = new SQLException("ERROR: some failure", sqlState);
    assertTrue(DatabaseWriter.isRethrownFromBatch(e, true));
    assertTrue(DatabaseWriter.isRethrownFromBatch(e, false));
    assertTrue(DatabaseWriter.isRethrownFromBatch(batchFailedWith(e), false));
  }

  @Test
  void batchIsRethrownOnMissingPartitionOnlyWhileRecoverable() {
    BatchUpdateException e = batchFailedWith(missingPartition());
    assertTrue(DatabaseWriter.isRethrownFromBatch(e, true));
    assertFalse(DatabaseWriter.isRethrownFromBatch(e, false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"23505", "XYZ49"})
  void batchFailsItsFeaturesOnOtherErrors(String sqlState) {
    SQLException e = new SQLException("ERROR: some failure", sqlState);
    assertFalse(DatabaseWriter.isRethrownFromBatch(e, true));
    assertFalse(DatabaseWriter.isRethrownFromBatch(batchFailedWith(e), true));
  }
}
