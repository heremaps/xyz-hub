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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.COMPOSITE_EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.X;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Iterating a composite space page by page with the nextPageToken must return the same features as one unpaged iteration, in every
 * context and for every page size, also when the composite has more features than one page.
 */
public class PSQLCompositeIterateIT extends LazyTablesITBase {
  private static final Set<String> SCHEMAS = ConcurrentHashMap.newKeySet();
  private static final List<SpaceContext> CONTEXTS = List.of(DEFAULT, EXTENSION, SUPER, COMPOSITE_EXTENSION, X);
  private static final List<Integer> PAGE_SIZES = List.of(1, 3, 10, 29);
  private static final int UNPAGED = 10_000;
  private static TestConnector connector;

  @Override
  protected Set<String> schemas() {
    return SCHEMAS;
  }

  @BeforeAll
  static void createConnector() throws Exception {
    connector = new PSQLCompositeIterateIT().newConnector();
  }

  @AfterAll
  static void cleanUp() throws Exception {
    dropSchemas(SCHEMAS);
  }

  /**
   * One level of a composite: its space ID, its table and, for a base, the version the level above is pinned to (null: HEAD).
   */
  private record Lvl(String spaceId, String table, Long pinnedVersion) {
    Lvl pinned(long version) {
      return new Lvl(spaceId, table, version);
    }
  }

  private Lvl level(String... ids) throws Exception {
    String table = newTable();
    createEager(connector, table);
    if (ids.length > 0)
      insertInto(connector, table, ids);
    return new Lvl(newId(), table, null);
  }

  /**
   * @param levels The composite, the space first and its bases after it
   */
  private static Map<String, Object> params(List<Lvl> levels) {
    Map<String, Object> params = null;
    for (int i = levels.size() - 1; i >= 0; i--) {
      Lvl level = levels.get(i);
      Map<String, Object> levelParams = new HashMap<>();
      levelParams.put(Space.TABLE_NAME, level.table());
      if (i > 0) {
        levelParams.put("spaceId", level.spaceId());
        if (level.pinnedVersion() != null)
          levelParams.put("version", level.pinnedVersion());
      }
      if (params != null)
        levelParams.put("extends", params);
      params = levelParams;
    }
    return params;
  }

  private static String[] ids(String prefix, int count) {
    return IntStream.range(0, count).mapToObj(i -> String.format("%s%02d", prefix, i)).toArray(String[]::new);
  }

  /**
   * Writes the features into the top level of the composite: new ones and overrides of inherited ones.
   */
  private static void write(List<Lvl> levels, String... ids) throws Exception {
    assertFeatureCollection(invoke(connector, writeFeaturesEvent("composite", params(levels), ids)));
  }

  /**
   * Deletes inherited features in the top level of the composite, which hides them.
   */
  private static void hide(List<Lvl> levels, String... ids) throws Exception {
    Map<String, String> deletes = new HashMap<>();
    for (String id : ids)
      deletes.put(id, null);
    FeatureCollection result = assertFeatureCollection(invoke(connector, new ModifyFeaturesEvent().withSpace("composite")
        .withParams(params(levels)).withContext(DEFAULT).withTransaction(true).withDeleteFeatures(deletes)));
    assertEquals(ids.length, result.getDeleted() == null ? 0 : result.getDeleted().size(), result.serialize());
  }

  private static long headVersion(Lvl level) throws Exception {
    return query(new SQLQuery("SELECT max(version) FROM ${schema}.${table}")
        .withVariable("schema", connector.schema())
        .withVariable("table", level.table()), rs -> rs.next() ? rs.getLong(1) : -1);
  }

  /**
   * Iterates to the end, following the nextPageToken.
   *
   * @return The IDs in the order of the pages
   */
  private static List<String> iterate(List<Lvl> levels, SpaceContext context, int limit) throws Exception {
    List<String> ids = new ArrayList<>();
    String token = null;
    int pages = 0;
    do {
      IterateFeaturesEvent<?> event = new IterateFeaturesEvent<>().withLimit(limit).withNextPageToken(token);
      event.setSpace("composite");
      event.setParams(params(levels));
      event.setContext(context);
      FeatureCollection page = assertFeatureCollection(invoke(connector, event));
      for (Feature feature : page.getFeatures())
        ids.add(feature.getId());
      token = page.getNextPageToken();
      assertTrue(++pages <= UNPAGED, "The iteration doesn't end");
    }
    while (token != null);
    return ids;
  }

  /**
   * Checks that every page size gives the unpaged result without duplicates in every context, and that the unpaged composed result
   * in context DEFAULT is the expected one.
   */
  private static void assertPagingIsComplete(List<Lvl> levels, Set<String> expectedInDefault) throws Exception {
    for (SpaceContext context : CONTEXTS) {
      //Sorted lists, not sets: a page must neither lose nor repeat a row. In context COMPOSITE_EXTENSION an overridden feature is
      //one row of the extension and one of its base, so an ID may occur twice there.
      List<String> unpaged = sorted(iterate(levels, context, UNPAGED));
      if (context == DEFAULT)
        assertEquals(List.copyOf(expectedInDefault), unpaged, "Unpaged iteration in context DEFAULT");
      for (int limit : PAGE_SIZES)
        assertEquals(unpaged, sorted(iterate(levels, context, limit)), "Context " + context + " with limit " + limit);
    }
  }

  private static List<String> sorted(List<String> ids) {
    List<String> sorted = new ArrayList<>(ids);
    sorted.sort(null);
    return sorted;
  }

  private static Set<String> expected(String[] base, String... adjustments) {
    Set<String> expected = new TreeSet<>(List.of(base));
    for (String adjustment : adjustments) {
      if (adjustment.startsWith("-"))
        expected.remove(adjustment.substring(1));
      else
        expected.add(adjustment);
    }
    return expected;
  }

  @Test
  public void oneLevel() throws Exception {
    String[] base = ids("b", 25);
    List<Lvl> levels = List.of(level(), level(base));
    write(levels, "x00", "x01", "b03");
    hide(levels, "b04", "b20");

    assertPagingIsComplete(levels, expected(base, "x00", "x01", "-b04", "-b20"));
  }

  @Test
  public void oneLevelOnBaseVersion() throws Exception {
    String[] base = ids("b", 25);
    Lvl root = level(base);
    List<Lvl> levels = List.of(level(), root.pinned(headVersion(root)));
    //Written after the pinned version: not part of the composite
    insertInto(connector, root.table(), "late0", "late1");
    write(levels, "x00", "x01", "b03");
    hide(levels, "b04", "b20");

    assertPagingIsComplete(levels, expected(base, "x00", "x01", "-b04", "-b20"));
  }

  @Test
  public void twoLevels() throws Exception {
    String[] base = ids("b", 25);
    Lvl root = level(base);
    Lvl intermediate = level();
    List<Lvl> intermediateComposite = List.of(intermediate, root);
    write(intermediateComposite, "i00", "i01", "i02", "i03", "b05");

    List<Lvl> levels = List.of(level(), intermediate, root);
    write(levels, "x00", "x01", "b07", "i01");
    hide(levels, "b08", "i02");

    assertPagingIsComplete(levels, expected(base, "i00", "i01", "i03", "x00", "x01", "-b08"));
  }

  @Test
  public void twoLevelsOnBaseVersions() throws Exception {
    String[] base = ids("b", 25);
    Lvl root = level(base);
    Lvl intermediate = level();
    List<Lvl> intermediateComposite = List.of(intermediate, root.pinned(headVersion(root)));
    write(intermediateComposite, "i00", "i01", "i02", "i03", "b05");
    insertInto(connector, root.table(), "late0");

    List<Lvl> levels = List.of(level(), intermediate.pinned(headVersion(intermediate)), intermediateComposite.get(1));
    write(intermediateComposite, "late1");
    write(levels, "x00", "x01", "b07", "i01");
    hide(levels, "b08", "i02");

    assertPagingIsComplete(levels, expected(base, "i00", "i01", "i03", "x00", "x01", "-b08"));
  }
}
