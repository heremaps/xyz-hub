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
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_SPACE_ID;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelState;
import com.here.xyz.util.db.pg.LazySpaceTables.TemplateProbe;
import com.here.xyz.util.db.pg.MaterializationTimeoutException.Phase;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper.TableComment;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class LazySpaceTablesTest {
  private static final String COMMENT = new TableComment(TEMPLATE_SPACE_ID, OLD_LAYOUT).serialize();

  private static Map<String, Object> level(String spaceId, String table, Object lazyTable, Map<String, Object> extendsMap) {
    Map<String, Object> level = new HashMap<>();
    if (spaceId != null)
      level.put("spaceId", spaceId);
    if (table != null)
      level.put("tableName", table);
    if (lazyTable != null)
      level.put("lazyTable", lazyTable);
    if (extendsMap != null)
      level.put("extends", extendsMap);
    return level;
  }

  //A two-level composite: root -> base1 -> base2, all lazy
  private static Map<String, Object> twoLevelParams() {
    return level(null, "t_root", true, level("base1", "t_base1", true, level("base2", "t_base2", true, null)));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extendsOf(Map<String, Object> level) {
    return (Map<String, Object>) level.get("extends");
  }

  @Test
  public void lazyLevelsOfAllDepths() {
    assertEquals(List.of(new Level(0, null, "t_root"), new Level(1, "base1", "t_base1"), new Level(2, "base2", "t_base2")),
        LazySpaceTables.lazyLevels(twoLevelParams()));
    assertTrue(LazySpaceTables.hasLazyLevel(twoLevelParams()));
  }

  @Test
  public void lazyLevelsIgnoresNonBooleanFlags() {
    Map<String, Object> params = level(null, "t_root", "true", level("base1", "t_base1", 1, level("base2", null, true, null)));

    assertEquals(List.of(new Level(2, "base2", null)), LazySpaceTables.lazyLevels(params));
    assertEquals(List.of(), LazySpaceTables.lazyLevels(level(null, "t_root", "true", null)));
    assertFalse(LazySpaceTables.hasLazyLevel(level(null, "t_root", false, level("base1", "t_base1", "true", null))));
    assertEquals(List.of(), LazySpaceTables.lazyLevels(null));
  }

  @Test
  public void substituteNoLevel() {
    Map<String, Object> params = twoLevelParams();
    Map<String, Object> substituted = LazySpaceTables.substitute(params, List.of());

    assertEquals(twoLevelParams(), substituted);
    assertNotSame(params, substituted);
    assertNotSame(extendsOf(params), extendsOf(substituted));
  }

  @Test
  public void substituteOneLevel() {
    Map<String, Object> params = level(null, "t_root", true, level("base1", "t_base1", null, null));
    Map<String, Object> substituted = LazySpaceTables.substitute(params, List.of(new Level(0, null, "t_root")));

    assertEquals(TEMPLATE_TABLE, substituted.get("tableName"));
    assertEquals(true, substituted.get("lazyTable"));
    assertEquals("t_base1", extendsOf(substituted).get("tableName"));
    assertEquals(level(null, "t_root", true, level("base1", "t_base1", null, null)), params, "The input must stay unchanged");
  }

  @Test
  public void substituteTwoLevels() {
    Map<String, Object> params = twoLevelParams();
    Map<String, Object> substituted = LazySpaceTables.substitute(params,
        List.of(new Level(1, "base1", "t_base1"), new Level(2, "base2", "t_base2")));

    assertEquals("t_root", substituted.get("tableName"));
    assertEquals(TEMPLATE_TABLE, extendsOf(substituted).get("tableName"));
    assertEquals(true, extendsOf(substituted).get("lazyTable"));
    assertEquals(TEMPLATE_TABLE, extendsOf(extendsOf(substituted)).get("tableName"));
    assertEquals(true, extendsOf(extendsOf(substituted)).get("lazyTable"));
    assertEquals("base2", extendsOf(extendsOf(substituted)).get("spaceId"));
    assertEquals(twoLevelParams(), params, "The input must stay unchanged");
  }

  @Test
  public void substituteLegacyLevelsWithoutTableName() {
    Map<String, Object> params = level(null, null, true, level("base1", null, true, null));
    List<Object> list = new ArrayList<>(List.of("a"));
    params.put("someList", list);
    Map<String, Object> substituted = LazySpaceTables.substitute(params, LazySpaceTables.lazyLevels(params));

    assertEquals(TEMPLATE_TABLE, substituted.get("tableName"));
    assertEquals(TEMPLATE_TABLE, extendsOf(substituted).get("tableName"));
    assertEquals(true, substituted.get("lazyTable"));
    assertEquals(true, extendsOf(substituted).get("lazyTable"));
    assertNull(params.get("tableName"), "The input must stay unchanged");
    assertNull(extendsOf(params).get("tableName"), "The input must stay unchanged");
    assertEquals(list, substituted.get("someList"));
    assertNotSame(list, substituted.get("someList"));
  }

  @Test
  public void validateOnDemandIndexes() {
    assertEquals(List.of("a", "b.c", "properties.x"),
        LazySpaceTables.validateOnDemandIndexes(Arrays.asList("a", "it's", "", "b.c", "back\\slash", null, 5, "properties.x")));
    assertEquals(List.of(), LazySpaceTables.validateOnDemandIndexes(null));
    assertEquals(List.of(), LazySpaceTables.validateOnDemandIndexes(List.of()));
  }

  @Test
  public void isReserved() {
    assertTrue(LazySpaceTables.isReserved(TEMPLATE_TABLE));
    assertTrue(LazySpaceTables.isReserved("xyz_empty_space_v2"));
    assertTrue(LazySpaceTables.isReserved("xyz_empty_space_"));
    assertFalse(LazySpaceTables.isReserved("xyz_empty_space"));
    assertFalse(LazySpaceTables.isReserved("my_xyz_empty_space_v1"));
    assertFalse(LazySpaceTables.isReserved("a1b2c3"));
    assertFalse(LazySpaceTables.isReserved(null));
  }

  @Test
  public void isTemplateWriteException() {
    assertTrue(LazySpaceTables.isTemplateWriteException(new SQLException("any message", "XYZ52")));
    assertTrue(LazySpaceTables.isTemplateWriteException(new SQLException(
        "ERROR: Internal error: write to the empty-space template rejected", "P0001")));
    assertFalse(LazySpaceTables.isTemplateWriteException(new SQLException("relation does not exist", "42P01")));
    assertFalse(LazySpaceTables.isTemplateWriteException(new SQLException()));
    assertFalse(LazySpaceTables.isTemplateWriteException(null));
  }

  @Test
  public void exhaustedTemplateSequenceIsATemplateWrite() {
    assertTrue(LazySpaceTables.isTemplateWriteException(new SQLException(
        "ERROR: nextval: reached maximum value of sequence \"xyz_empty_space_v1_version_seq\" (1)", "2200H")));
    assertTrue(LazySpaceTables.isTemplateWriteException(new SQLException(
        "ERROR: nextval: reached maximum value of sequence \\\"xyz_empty_space_v1_i_seq\\\" (1)", "2200H")));
    //A real space's sequence, even one whose name contains the template's name
    assertFalse(LazySpaceTables.isTemplateWriteException(new SQLException(
        "ERROR: nextval: reached maximum value of sequence \"abc_version_seq\" (1)", "2200H")));
    assertFalse(LazySpaceTables.isTemplateWriteException(new SQLException(
        "ERROR: nextval: reached maximum value of sequence \"my_xyz_empty_space_v1_version_seq\" (1)", "2200H")));
    //The template's name with another SQLSTATE
    assertFalse(LazySpaceTables.isTemplateWriteException(new SQLException(
        "ERROR: relation \"xyz_empty_space_v1_version_seq\" does not exist", "42P01")));
    assertFalse(LazySpaceTables.isTemplateWriteException(new SQLException(null, "2200H")));

    BatchUpdateException batchError = new BatchUpdateException("Batch entry 0 was aborted", "2200H", new int[0]);
    batchError.setNextException(new SQLException(
        "ERROR: nextval: reached maximum value of sequence \"xyz_empty_space_v1_version_seq\" (1)", "2200H"));
    assertTrue(LazySpaceTables.isTemplateWriteError(batchError));
  }

  @Test
  public void isTemplateWriteErrorWalksCausesAndNextExceptions() {
    SQLException guardError = new SQLException("guard", "XYZ52");

    assertTrue(LazySpaceTables.isTemplateWriteError(guardError));
    assertTrue(LazySpaceTables.isTemplateWriteError(new RuntimeException(new IllegalStateException(guardError))));

    BatchUpdateException batchError = new BatchUpdateException("Batch entry 0 was aborted", "XX000", new int[0]);
    batchError.setNextException(new SQLException("ERROR: " + LazySpaceTables.TEMPLATE_WRITE_MESSAGE, "P0001"));
    assertTrue(LazySpaceTables.isTemplateWriteError(batchError));
    assertTrue(LazySpaceTables.isTemplateWriteError(new RuntimeException(batchError)));

    BatchUpdateException otherBatchError = new BatchUpdateException("Batch entry 0 was aborted", "23505", new int[0]);
    otherBatchError.setNextException(new SQLException("duplicate key", "23505"));
    assertFalse(LazySpaceTables.isTemplateWriteError(otherBatchError));
    assertFalse(LazySpaceTables.isTemplateWriteError(new RuntimeException("no SQL error")));
    assertFalse(LazySpaceTables.isTemplateWriteError(null));
  }

  @Test
  public void exceptionsCarryTheirSqlState() {
    MaterializationTimeoutException lockTimeout = new MaterializationTimeoutException("lock", "55P03", Phase.LOCK, null);
    assertEquals("55P03", lockTimeout.getSQLState());
    assertEquals(Phase.LOCK, lockTimeout.getPhase());

    SQLException cause = new SQLException("canceled", "57014");
    MaterializationTimeoutException ddlTimeout = new MaterializationTimeoutException("ddl", "57014", Phase.DDL, cause);
    assertEquals("57014", ddlTimeout.getSQLState());
    assertEquals(Phase.DDL, ddlTimeout.getPhase());
    assertSame(cause, ddlTimeout.getCause());

    ReservedTableNameException reserved = new ReservedTableNameException(TEMPLATE_TABLE);
    assertEquals("XX000", reserved.getSQLState());
    assertTrue(reserved.getMessage().contains(TEMPLATE_TABLE));

    assertEquals("XX000", TemplateUnavailableException.missing("public").getSQLState());
    assertEquals("XX000", TemplateUnavailableException.notIdentical("public").getSQLState());
  }

  @Test
  public void templateUnavailableMessages() {
    String missing = TemplateUnavailableException.missing("s1").getMessage();
    String notIdentical = TemplateUnavailableException.notIdentical("s1").getMessage();

    assertTrue(missing.startsWith("Empty-space template missing"));
    assertEquals("Empty-space template missing: xyz_empty_space_v1 in schema s1", missing);
    assertTrue(notIdentical.startsWith("Empty-space template not identical"));
    assertEquals("Empty-space template not identical: xyz_empty_space_v1 in schema s1", notIdentical);
  }

  @Test
  public void templateProbe() {
    TemplateProbe absent = new TemplateProbe(false, false, null, 0, false);
    assertTrue(absent.isAbsent());
    assertFalse(absent.isIdentical());

    TemplateProbe identical = new TemplateProbe(true, true, COMMENT, 3, true);
    assertFalse(identical.isAbsent());
    assertTrue(identical.isIdentical());

    TemplateProbe missingGuardTrigger = new TemplateProbe(true, true, COMMENT, 2, true);
    assertFalse(missingGuardTrigger.isAbsent());
    assertFalse(missingGuardTrigger.isIdentical());

    String otherComment = new TableComment("some-space", OLD_LAYOUT).serialize();
    assertFalse(new TemplateProbe(true, true, otherComment, 3, true).isIdentical());
    assertFalse(new TemplateProbe(true, true, "{not json", 3, true).isIdentical());
    assertFalse(new TemplateProbe(true, true, "[\"" + TEMPLATE_SPACE_ID + "\"]", 3, true).isIdentical());
    assertFalse(new TemplateProbe(true, true, null, 3, true).isIdentical());

    TemplateProbe usableSequence = new TemplateProbe(true, true, COMMENT, 3, false);
    assertFalse(usableSequence.isAbsent());
    assertFalse(usableSequence.isIdentical());

    TemplateProbe foreignRelationOnly = new TemplateProbe(true, false, null, 0, false);
    assertFalse(foreignRelationOnly.isAbsent());
    assertFalse(foreignRelationOnly.isIdentical());
  }

  @Test
  public void templateProbeDifferences() {
    assertEquals(List.of(), new TemplateProbe(true, true, COMMENT, 3, true).differences());
    assertEquals(List.of("root table missing", "0 of 3 guard triggers", "version sequence not exhausted"),
        new TemplateProbe(true, false, null, 0, false).differences());
    assertEquals(List.of("comment {not json"), new TemplateProbe(true, true, "{not json", 3, true).differences());
  }

  @Test
  public void requireTemplateWithoutSubstitutedLevelsNeverThrows() throws Exception {
    LevelCheck missingTarget = new LevelCheck(List.of(LevelState.NEVER_MATERIALIZED), false);
    LazySpaceTables.requireTemplate("s1", missingTarget, Set.of());
    LazySpaceTables.requireTemplate("s1", new LevelCheck(List.of(LevelState.NEVER_MATERIALIZED), true), Set.of());
  }

  @Test
  public void requireTemplateThrowsForASubstitutedLevelWithoutTheTemplate() throws Exception {
    LevelCheck missingBase = new LevelCheck(List.of(LevelState.EXISTS, LevelState.NEVER_MATERIALIZED), false);
    TemplateUnavailableException e = assertThrows(TemplateUnavailableException.class,
        () -> LazySpaceTables.requireTemplate("s1", missingBase, Set.of(1)));
    assertEquals("XX000", e.getSQLState());
    assertEquals("Empty-space template missing: " + TEMPLATE_TABLE + " in schema s1", e.getMessage());

    LazySpaceTables.requireTemplate("s1", new LevelCheck(List.of(LevelState.EXISTS, LevelState.NEVER_MATERIALIZED), true),
        Set.of(1));
  }
}
