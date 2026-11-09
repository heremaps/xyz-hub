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

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.SQLError.EMPTY_TEMPLATE_WRITE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.hub.Space;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helpers for lazy spaces: spaces whose tables are created by their first write. Until then, reads of such a space
 * use the empty-space template, a set of empty tables per connector schema that rejects every write.
 * Shared by the connector and the job steps.
 */
public class LazySpaceTables {
  /**
   * The name of the empty-space template's root table. The version allows a future template with other columns.
   */
  public static final String TEMPLATE_TABLE = "xyz_empty_space_v1";
  /**
   * No space table may have a name with this prefix.
   */
  public static final String RESERVED_PREFIX = "xyz_empty_space_";
  /**
   * The space ID in the table comment of the template.
   */
  public static final String TEMPLATE_SPACE_ID = "xyz-empty-space-template";
  /**
   * The event parameter with the on-demand indexes to create when a write materializes the table.
   */
  public static final String ON_DEMAND_INDEXES = "onDemandIndexes";
  /**
   * The message of the error that the guard triggers of the template raise, with SQLSTATE
   * {@link SQLError#EMPTY_TEMPLATE_WRITE}.
   */
  public static final String TEMPLATE_WRITE_MESSAGE = "Internal error: write to the empty-space template rejected";
  /**
   * The guard triggers of the template: one on the root, one on the head table and one on the first partition.
   */
  public static final int GUARD_TRIGGER_COUNT = 3;
  private static final String TEMPLATE_WRITE_MESSAGE_PART = "empty-space template rejected";
  private static final String EXTENDS = "extends";
  private static final String SPACE_ID = "spaceId";
  private static final int MAX_DEPTH = 2;

  private LazySpaceTables() {}

  /**
   * One level of a space's composition.
   *
   * @param depth 0 for the space itself, 1 for the space it extends, 2 for the space that one extends
   * @param spaceId The space ID if the level's params carry it (the extended levels do), otherwise null
   * @param table The level's physical table name, or null for a level without an explicit table name
   */
  public record Level(int depth, String spaceId, String table) {}

  /**
   * The state of the table of a level that carries the lazyTable flag. A level whose flag was cleared is not lazy and
   * has no state.
   */
  public enum LevelState {
    /**
     * The table exists.
     */
    EXISTS,
    /**
     * No table on a level that still carries lazyTable: never written, or lost while its flag was not cleared yet;
     * the level reads as the template and the next write materializes it.
     */
    NEVER_MATERIALIZED
  }

  /**
   * The result of one existence check.
   *
   * @param states One state per checked level, in the order of the checked levels
   * @param templatePresent Whether the root table of the template exists
   */
  public record LevelCheck(List<LevelState> states, boolean templatePresent) {}

  /**
   * What the catalog says about the relations of the template.
   *
   * @param anyRelation Whether any relation with a template name exists
   * @param rootExists Whether the template's root table exists
   * @param comment The raw comment of the root table, or null
   * @param guardTriggers The number of enabled guard triggers
   * @param sequencesExhausted Whether the version sequence of the template is exhausted
   */
  public record TemplateProbe(boolean anyRelation, boolean rootExists, String comment, int guardTriggers,
      boolean sequencesExhausted) {

    /**
     * @return Whether no relation of the template exists, so it can be installed
     */
    public boolean isAbsent() {
      return !anyRelation;
    }

    /**
     * @return Whether the template is complete and was created by the installer
     */
    public boolean isIdentical() {
      return rootExists && guardTriggers == GUARD_TRIGGER_COUNT && sequencesExhausted
          && TEMPLATE_SPACE_ID.equals(commentSpaceId(comment));
    }

    //An unparsable comment yields null and thus a template that is not identical
    private static Object commentSpaceId(String comment) {
      if (comment == null)
        return null;
      try {
        Map<?, ?> parsed = XyzSerializable.deserialize(comment, Map.class);
        return parsed == null ? null : parsed.get(SPACE_ID);
      }
      catch (JsonProcessingException | RuntimeException e) {
        return null;
      }
    }
  }

  /**
   * The set of tables and indexes that a materialization creates.
   */
  public enum Profile {
    /**
     * Everything an eager space creation creates, including the on-demand indexes.
     */
    FULL,
    /**
     * The tables with only the system indexes a bulk load keeps, for an import that creates the other indexes later.
     */
    BARE
  }

  public enum MaterializationResult {
    /**
     * The space is not lazy, nothing was checked.
     */
    NOT_LAZY,
    /**
     * The table existed already.
     */
    EXISTED,
    /**
     * The table was created by this call.
     */
    CREATED
  }

  /**
   * @param table A physical table name
   * @return Whether the name has the prefix that is reserved for the empty-space template
   */
  public static boolean isReserved(String table) {
    return table != null && table.startsWith(RESERVED_PREFIX);
  }

  /**
   * Returns the levels of the provided params that carry the lazyTable flag ({@link Space#isLazyTable(Map)}): the params
   * themselves, "extends" and "extends.extends". A level with another storage ID than the composite carries no flag.
   *
   * @param params The event params or the resolved space params
   * @return The lazy levels, ordered by depth
   */
  public static List<Level> lazyLevels(Map<String, Object> params) {
    List<Level> levels = new ArrayList<>();
    Map<String, Object> level = params;
    for (int depth = 0; depth <= MAX_DEPTH && level != null; depth++) {
      if (Space.isLazyTable(level))
        levels.add(new Level(depth, stringOrNull(level.get(SPACE_ID)), stringOrNull(level.get(TABLE_NAME))));
      level = asMap(level.get(EXTENDS));
    }
    return levels;
  }

  public static boolean hasLazyLevel(Map<String, Object> params) {
    return !lazyLevels(params).isEmpty();
  }

  /**
   * Returns a deep copy of the params, in which the table name of each provided level is the template.
   * The lazyTable flags stay, the provided params are not changed.
   *
   * @param params The event params
   * @param missingLevels The levels to read from the template
   * @return The substituted copy
   */
  public static Map<String, Object> substitute(Map<String, Object> params, Collection<Level> missingLevels) {
    Map<String, Object> copy = deepCopy(params);
    for (Level missingLevel : missingLevels) {
      Map<String, Object> level = copy;
      for (int depth = 0; depth < missingLevel.depth() && level != null; depth++)
        level = asMap(level.get(EXTENDS));
      if (level == null)
        throw new IllegalArgumentException("The params have no level of depth " + missingLevel.depth());
      level.put(TABLE_NAME, TEMPLATE_TABLE);
    }
    return copy;
  }

  /**
   * Applies the character rules of the space configuration to the on-demand indexes of an event, because they reach the
   * index creation without that validation.
   *
   * @param onDemandIndexes The property paths, may be null
   * @return The non-empty strings without ' and \, in their original order
   */
  public static List<String> validateOnDemandIndexes(List<?> onDemandIndexes) {
    if (onDemandIndexes == null)
      return List.of();
    return onDemandIndexes.stream()
        .filter(index -> index instanceof String property && !property.isEmpty() && !property.contains("'")
            && !property.contains("\\"))
        .map(index -> (String) index)
        .toList();
  }

  /**
   * Checks one exception, without walking its causes.
   *
   * @return Whether the exception was raised by a guard trigger of the template
   */
  public static boolean isTemplateWriteException(SQLException e) {
    return e != null && (EMPTY_TEMPLATE_WRITE.errorCode.equals(e.getSQLState())
        || e.getMessage() != null && e.getMessage().contains(TEMPLATE_WRITE_MESSAGE_PART));
  }

  /**
   * Walks the causes and, for each {@link SQLException}, the chain of {@link SQLException#getNextException()}, because a
   * batch error may carry the guard error only in that chain.
   *
   * @return Whether any of them was raised by a guard trigger of the template
   */
  public static boolean isTemplateWriteError(Throwable throwable) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    if (throwable != null)
      pending.push(throwable);
    while (!pending.isEmpty()) {
      Throwable current = pending.pop();
      if (!seen.add(current))
        continue;
      if (current instanceof SQLException sqlException) {
        if (isTemplateWriteException(sqlException))
          return true;
        if (sqlException.getNextException() != null)
          pending.push(sqlException.getNextException());
      }
      if (current.getCause() != null)
        pending.push(current.getCause());
    }
    return false;
  }

  private static String stringOrNull(Object value) {
    return value instanceof String string ? string : null;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : null;
  }

  @SuppressWarnings("unchecked")
  private static <T> T deepCopy(T value) {
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> copy = new LinkedHashMap<>();
      map.forEach((key, entry) -> copy.put(key, deepCopy(entry)));
      return (T) copy;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>();
      list.forEach(entry -> copy.add(deepCopy(entry)));
      return (T) copy;
    }
    return value;
  }
}
