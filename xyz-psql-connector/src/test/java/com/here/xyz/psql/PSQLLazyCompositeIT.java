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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Composite spaces with lazy levels. Each read of a composite with a never written lazy level must give what the same composite
 * gives with an eagerly created empty table on that level.
 */
public class PSQLLazyCompositeIT extends LazyTablesITBase {
  private static final Set<String> SCHEMAS = ConcurrentHashMap.newKeySet();
  private static final List<String> IDS = List.of("b1", "b2", "i1", "x1");
  private static TestConnector connector;

  @Override
  protected Set<String> schemas() {
    return SCHEMAS;
  }

  @BeforeAll
  static void createConnector() throws Exception {
    connector = new PSQLLazyCompositeIT().newConnector().with("lazyTableCreation", true);
  }

  @AfterAll
  static void cleanUp() throws Exception {
    dropSchemas(SCHEMAS);
  }

  /**
   * One level of a composite: its table, whether it carries the flag, and the IDs written to it (none for a lazy level).
   */
  private record Lvl(String spaceId, String table, boolean lazy) {}

  private Lvl eager(String... ids) throws Exception {
    String table = newTable();
    createEager(connector, table);
    if (ids.length > 0)
      insertInto(connector, table, ids);
    return new Lvl(newId(), table, false);
  }

  private Lvl lazy() {
    return new Lvl(newId(), newTable(), true);
  }

  /**
   * The eager twin of a lazy level: an eagerly created empty table instead of the missing one.
   */
  private Lvl twin(Lvl level) throws Exception {
    return level.lazy() ? eager() : level;
  }

  private static Map<String, Object> params(List<Lvl> levels) {
    Map<String, Object> params = null;
    for (int i = levels.size() - 1; i >= 0; i--) {
      Lvl level = levels.get(i);
      Map<String, Object> levelParams = new HashMap<>();
      levelParams.put(Space.TABLE_NAME, level.table());
      if (level.lazy())
        levelParams.put(Space.LAZY_TABLE, true);
      if (i > 0)
        levelParams.put("spaceId", level.spaceId());
      if (params != null)
        levelParams.put("extends", params);
      params = levelParams;
    }
    return params;
  }

  private static Set<String> ids(FeatureCollection collection) throws Exception {
    Set<String> ids = new TreeSet<>();
    for (Feature feature : collection.getFeatures())
      ids.add(feature.getId());
    return ids;
  }

  private static List<Supplier<ContextAwareEvent<?>>> reads() {
    return List.of(() -> new GetFeaturesByIdEvent().withIds(IDS), () -> new IterateFeaturesEvent<>().withLimit(100));
  }

  /**
   * Compares the reads of the composite (the space first, its bases after it) with those of its eager twin in every context.
   */
  private void assertParityWithEagerTwin(List<Lvl> levels, Set<String> expectedInDefault) throws Exception {
    List<Lvl> twins = new java.util.ArrayList<>();
    for (Lvl level : levels)
      twins.add(twin(level));
    String spaceId = newId();
    for (SpaceContext context : List.of(DEFAULT, EXTENSION, SUPER))
      for (Supplier<ContextAwareEvent<?>> read : reads()) {
        ContextAwareEvent<?> eagerEvent = read.get();
        eagerEvent.setSpace(spaceId);
        eagerEvent.setParams(params(twins));
        eagerEvent.setContext(context);
        ContextAwareEvent<?> lazyEvent = read.get();
        lazyEvent.setSpace(spaceId);
        lazyEvent.setParams(params(levels));
        lazyEvent.setContext(context);
        Set<String> expected = ids(assertFeatureCollection(invoke(connector, eagerEvent)));
        if (context == DEFAULT)
          assertEquals(expectedInDefault, expected, "The eager twin in context DEFAULT");
        assertEquals(expected, ids(assertFeatureCollection(invoke(connector, lazyEvent))),
            eagerEvent.getClass().getSimpleName() + " in context " + context);
      }
    for (Lvl level : levels)
      if (level.lazy())
        assertEquals(Set.of(), relations(connector.schema(), level.table()), "A read must not create a table");
  }

  @Test
  public void lazyExtensionOfAnEagerBase() throws Exception {
    assertParityWithEagerTwin(List.of(lazy(), eager("b1", "b2")), Set.of("b1", "b2"));
  }

  @Test
  public void eagerExtensionOfALazyBase() throws Exception {
    assertParityWithEagerTwin(List.of(eager("x1"), lazy()), Set.of("x1"));
  }

  @Test
  public void lazyExtensionOfALazyBase() throws Exception {
    assertParityWithEagerTwin(List.of(lazy(), lazy()), Set.of());
  }

  @Test
  public void twoLevelsWithLazyExtension() throws Exception {
    assertParityWithEagerTwin(List.of(lazy(), eager("i1"), eager("b1", "b2")), Set.of("b1", "b2", "i1"));
  }

  @Test
  public void twoLevelsWithLazyBases() throws Exception {
    assertParityWithEagerTwin(List.of(eager("x1"), lazy(), lazy()), Set.of("x1"));
  }

  @Test
  public void twoLevelsAllLazy() throws Exception {
    assertParityWithEagerTwin(List.of(lazy(), lazy(), lazy()), Set.of());
  }

  @Test
  public void twoLevelsWithLazyIntermediate() throws Exception {
    assertParityWithEagerTwin(List.of(eager("x1"), lazy(), eager("b1", "b2")), Set.of("b1", "b2", "x1"));
  }
}
