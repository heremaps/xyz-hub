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

package com.here.xyz.util.db.pg;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ScriptInstallerTest {

  private static SQLException error(String sqlState) {
    return new SQLException("Some error", sqlState);
  }

  @Test
  public void connectionErrorsAreTransient() {
    for (String sqlState : List.of("08000", "08001", "08003", "08004", "08006", "08P01"))
      assertTrue(sqlState, ScriptInstaller.isTransient(error(sqlState)));
  }

  @Test
  public void listedSqlStatesAreTransient() {
    for (String sqlState : List.of("57014", "55P03", "57P01", "57P02", "57P03", "53300", "40001", "40P01", "23505", "25006"))
      assertTrue(sqlState, ScriptInstaller.isTransient(error(sqlState)));
  }

  @Test
  public void concurrentlyUpdatedTupleIsTransient() {
    assertTrue(ScriptInstaller.isTransient(new SQLException("ERROR: tuple concurrently updated", "XX000")));
  }

  @Test
  public void timeoutsAndFailedExecutionAttemptsAreTransient() {
    assertTrue(ScriptInstaller.isTransient(new SQLTimeoutException("Timed out")));
    assertTrue(ScriptInstaller.isTransient(error("XYZ51")));
    //SQLQuery throws its FailedExecutionAttempt with SQL state 54000 if no time is left, which is not a transient state on its own
    assertTrue(ScriptInstaller.isTransient(new FailedExecutionAttempt("No time left to execute query.", "54000")));
  }

  @Test
  public void transientCauseIsFound() {
    assertTrue(ScriptInstaller.isTransient(new SQLException("Wrapper", "XX000", error("57014"))));
    assertTrue(ScriptInstaller.isTransient(new SQLException("Wrapper", null, new RuntimeException(error("08006")))));
  }

  @Test
  public void otherErrorsArePermanent() {
    for (String sqlState : List.of("42601", "42883", "42809", "42501", "54000", "XX000"))
      assertFalse(sqlState, ScriptInstaller.isTransient(error(sqlState)));
    assertFalse(ScriptInstaller.isTransient(new SQLException("ERROR: cache lookup failed for function 12345", "XX000")));
    assertFalse(ScriptInstaller.isTransient(new SQLException("Some error without state")));
    assertFalse(ScriptInstaller.isTransient(new SQLException("Wrapper", "42601", error("42883"))));
  }

  /**
   * Has the simple name of SQLQuery's FailedExecutionAttempt, which can only be created by SQLQuery's execution context.
   */
  private static class FailedExecutionAttempt extends SQLException {
    FailedExecutionAttempt(String message, String sqlState) {
      super(message, sqlState);
    }
  }
}
