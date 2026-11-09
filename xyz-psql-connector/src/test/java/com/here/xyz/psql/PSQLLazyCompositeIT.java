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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.LazySpaceTables;
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

  private static Set<String> readIds(List<Lvl> levels, SpaceContext context) throws Exception {
    return ids(assertFeatureCollection(invoke(connector, new GetFeaturesByIdEvent().withIds(IDS).withSpace("composite")
        .withParams(params(levels)).withContext(context))));
  }

  @Test
  public void deleteInTheLazyExtensionOfAnEagerBase() throws Exception {
    Lvl extension = lazy();
    List<Lvl> levels = List.of(extension, eager("b1", "b2"));
    List<Lvl> twins = List.of(eager(), levels.get(1));

    for (List<Lvl> composite : List.of(levels, twins)) {
      ModifyFeaturesEvent delete = new ModifyFeaturesEvent().withSpace("composite").withParams(params(composite))
          .withContext(DEFAULT).withTransaction(true).withDeleteFeatures(new HashMap<>(Map.of("b1", "0")));
      FeatureCollection result = assertFeatureCollection(invoke(connector, delete));
      assertEquals(List.of("b1"), result.getDeleted(), result.serialize());
    }
    assertEquals(Set.of("b2"), readIds(levels, DEFAULT));
    for (SpaceContext context : List.of(DEFAULT, EXTENSION, SUPER))
      assertEquals(readIds(twins, context), readIds(levels, context), "Context " + context);
    assertEquals(2, countRows(connector.schema(), levels.get(1).table()), "The base keeps the feature");
    assertTrue(relationExists(connector.schema(), extension.table()), "The delete materializes the extension");
  }

  @Test
  public void superWriteMaterializesTheBase() throws Exception {
    Lvl base = lazy();
    List<Lvl> levels = List.of(eager("x1"), base);
    FeatureCollection result = assertFeatureCollection(invoke(connector,
        writeFeaturesEvent("composite", params(levels), "b9").withContext(SUPER)));
    assertEquals(Boolean.TRUE, result.getMaterialized());
    assertTrue(relationExists(connector.schema(), base.table()));
    assertEquals(1, countRows(connector.schema(), base.table()), "The write goes into the base");
    assertEquals(1, countRows(connector.schema(), levels.get(0).table()), "The extension keeps its own feature only");
  }

  @Test
  public void inheritedIndexesAtMaterialization() throws Exception {
    Lvl extension = lazy();
    List<Lvl> levels = List.of(extension, eager("b1"));
    Map<String, Object> params = params(levels);
    //The Hub sends the effective searchable properties, which an extension inherits from its root
    params.put(LazySpaceTables.ON_DEMAND_INDEXES, List.of("foo"));
    assertFeatureCollection(invoke(connector, writeFeaturesEvent("composite", params, "x1")));

    String eagerTable = newTable();
    String eagerSpace = newId();
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, eagerSpace, eagerParams(eagerTable))
        .withSpaceDefinition(new Space().withId(eagerSpace).withSearchableProperties(Map.of("foo", true)))));
    Set<String> eagerRows = catalogRows(connector.schema(), eagerTable);
    assertTrue(eagerRows.stream().anyMatch(row -> row.contains("foo")), eagerRows.toString());
    assertEquals(eagerRows, catalogRows(connector.schema(), extension.table()));
  }

  //------------ Statistics and the search guard ------------

  private static Map<String, Object> comparable(XyzResponse response) throws Exception {
    Map<String, Object> json = XyzSerializable.deserialize(response.serialize(), new TypeReference<Map<String, Object>>() {});
    json.remove("streamId");
    return json;
  }

  private void assertStatisticsParity(List<Lvl> levels) throws Exception {
    List<Lvl> twins = new java.util.ArrayList<>();
    for (Lvl level : levels)
      twins.add(twin(level));
    for (boolean fastMode : List.of(false, true))
      for (SpaceContext context : List.of(DEFAULT, EXTENSION)) {
        XyzResponse eager = invoke(connector, new GetStatisticsEvent().withFastMode(fastMode).withSpace("composite")
            .withParams(params(twins)).withContext(context));
        XyzResponse lazy = invoke(connector, new GetStatisticsEvent().withFastMode(fastMode).withSpace("composite")
            .withParams(params(levels)).withContext(context));
        assertTrue(eager instanceof StatisticsResponse, eager.serialize());
        assertEquals(comparable(eager), comparable(lazy), "fastMode " + fastMode + ", context " + context);
      }
  }

  @Test
  public void fullStatisticsOfALazyExtensionOfAnEagerBase() throws Exception {
    assertStatisticsParity(List.of(lazy(), eager("b1", "b2")));
  }

  @Test
  public void fullStatisticsOfAnEagerExtensionOfALazyBase() throws Exception {
    assertStatisticsParity(List.of(eager("x1"), lazy()));
  }

  @Test
  public void fullStatisticsOfALazyExtensionOfALazyBase() throws Exception {
    assertStatisticsParity(List.of(lazy(), lazy()));
  }

  /**
   * A table above the size threshold of the search guard, with an on-demand index on "foo" like every level of a composite whose
   * root has "foo" as searchable property.
   */
  private Lvl big() throws Exception {
    String table = newTable();
    String spaceId = newId();
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, spaceId, eagerParams(table))
        .withSpaceDefinition(new Space().withId(spaceId).withSearchableProperties(Map.of("foo", true)))));
    execute(new SQLQuery("""
        INSERT INTO ${schema}.${table} (id, version, operation, jsondata)
          SELECT 'g' || n, 1, 'I', jsonb_build_object('id', 'g' || n, 'type', 'Feature',
            'properties', jsonb_build_object('foo', n, 'bar', n))
          FROM generate_series(1, 10100) n""")
        .withVariable("schema", connector.schema())
        .withVariable("table", table));
    execute(new SQLQuery("ANALYZE ${schema}.${table}").withVariable("schema", connector.schema()).withVariable("table", table));
    return new Lvl(spaceId, table, false);
  }

  /**
   * An eager twin that has the inherited on-demand index too.
   */
  private Lvl indexedTwin(Lvl level) throws Exception {
    if (!level.lazy())
      return level;
    String table = newTable();
    String spaceId = newId();
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, spaceId, eagerParams(table))
        .withSpaceDefinition(new Space().withId(spaceId).withSearchableProperties(Map.of("foo", true)))));
    return new Lvl(spaceId, table, false);
  }

  private static SearchForFeaturesEvent<?> search(String property) {
    PropertyQueryList list = new PropertyQueryList();
    list.add(new PropertyQuery().withKey("properties." + property).withOperation(QueryOperation.EQUALS).withValues(List.of(5)));
    PropertiesQuery query = new PropertiesQuery();
    query.add(list);
    return new SearchForFeaturesEvent<>().withPropertiesQuery(query).withLimit(10);
  }

  private static Object searchResult(XyzResponse response) throws Exception {
    if (response instanceof ErrorResponse error)
      return error.getError() + ": " + error.getErrorMessage();
    return ids(assertFeatureCollection(response));
  }

  private void assertSearchGuardParity(List<Lvl> levels, boolean unindexedAllowed) throws Exception {
    List<Lvl> twins = new java.util.ArrayList<>();
    for (Lvl level : levels)
      twins.add(indexedTwin(level));
    for (String property : List.of("foo", "bar")) {
      SearchForFeaturesEvent<?> eagerEvent = search(property);
      eagerEvent.setSpace("composite");
      eagerEvent.setParams(params(twins));
      SearchForFeaturesEvent<?> lazyEvent = search(property);
      lazyEvent.setSpace("composite");
      lazyEvent.setParams(params(levels));
      Object eager = searchResult(invoke(connector, eagerEvent));
      assertEquals(eager, searchResult(invoke(connector, lazyEvent)), "Search on " + property);
      if ("bar".equals(property))
        assertEquals(unindexedAllowed, !(eager instanceof String), "Search on the unindexed property: " + eager);
      else
        assertTrue(eager instanceof Set, "Search on the indexed property: " + eager);
    }
  }

  @Test
  public void searchGuardOfALazyExtensionOfABigBase() throws Exception {
    assertSearchGuardParity(List.of(lazy(), big()), false);
  }

  @Test
  public void searchGuardOfABigExtensionOfALazyBase() throws Exception {
    assertSearchGuardParity(List.of(big(), lazy()), false);
  }

  @Test
  public void searchGuardOfALazyExtensionOfALazyBase() throws Exception {
    assertSearchGuardParity(List.of(lazy(), lazy()), true);
  }

  @Test
  public void searchGuardOfALazyExtensionOfABigIntermediateAndALazyRoot() throws Exception {
    assertSearchGuardParity(List.of(lazy(), big(), lazy()), false);
  }

  @Test
  public void twoLevelsWithLazyIntermediate() throws Exception {
    assertParityWithEagerTwin(List.of(eager("x1"), lazy(), eager("b1", "b2")), Set.of("b1", "b2", "x1"));
  }
}
