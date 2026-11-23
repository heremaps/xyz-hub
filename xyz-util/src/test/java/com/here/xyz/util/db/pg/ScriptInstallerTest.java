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

import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.pg.LazySpaceTables.TemplateProbe;
import com.here.xyz.util.db.pg.ScriptInstaller.State;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.TableComment;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  @Test
  public void templatesWithoutACheckFollowTheScripts() {
    assertEquals(Map.of(LazySpaceTables.TEMPLATE_TABLE, TemplateCheck.NOT_VERIFIED), ScriptInstaller.uncheckedTemplates(State.TRANSIENT,
        "s1"));
    assertEquals(Map.of(LazySpaceTables.TEMPLATE_TABLE, TemplateCheck.FAILED_PERMANENT),
        ScriptInstaller.uncheckedTemplates(State.PERMANENT, "s1"));
    assertEquals("Without a template schema no template is requested", Map.of(),
        ScriptInstaller.uncheckedTemplates(State.TRANSIENT, null));
    assertEquals("One result per requested version", ScriptInstaller.TEMPLATE_VERSIONS,
        List.copyOf(ScriptInstaller.uncheckedTemplates(State.TRANSIENT, "s1").keySet()));
  }

  @Test
  public void classifyAnExistingTemplate() {
    String comment = new TableComment(LazySpaceTables.TEMPLATE_SPACE_ID, OLD_LAYOUT).serialize();
    assertEquals(TemplateCheck.VERIFIED, ScriptInstaller.classify(new TemplateProbe(true, true, comment, 3, true)));
    assertEquals(TemplateCheck.NOT_IDENTICAL, ScriptInstaller.classify(new TemplateProbe(true, false, null, 0, false)));
    assertEquals(TemplateCheck.NOT_IDENTICAL, ScriptInstaller.classify(new TemplateProbe(true, true, comment, 3, false)));
  }

  @Test
  public void compileSearchPathAsTheConnectionsGetIt() {
    //The path XyzConnectionCustomizer.onAcquire set before the method was extracted
    List<String> scriptSchemas = List.of("hub.common", "hub.ext");
    List<String> enrichedSearchPath = new ArrayList<>(List.of("lazy_abc", "public", "topology"));
    enrichedSearchPath.addAll(scriptSchemas);
    String before = enrichedSearchPath.stream().map(schema -> "\"" + schema + "\"").collect(Collectors.joining(", "));

    assertEquals(before, PooledDataSources.compileSearchPath("lazy_abc", scriptSchemas));
    assertEquals("\"lazy_abc\", \"public\", \"topology\", \"hub.common\", \"hub.ext\"",
        PooledDataSources.compileSearchPath("lazy_abc", scriptSchemas));
    assertEquals("\"public\", \"public\", \"topology\"", PooledDataSources.compileSearchPath("public", List.of()));
  }

  @Test
  public void compileSearchPathDoublesEmbeddedQuotes() {
    assertEquals("\"a\"\"b\", \"public\", \"topology\", \"x\"\"\"\"y\"",
        PooledDataSources.compileSearchPath("a\"b", List.of("x\"\"y")));
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
