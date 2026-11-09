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

import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_WRITE_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.DeleteChangesetsEvent;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.IterateChangesetsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The connector side of lazy space tables, and the error handling of writes that applies to every space: a write that reaches
 * the empty-space template or a missing table never reports success.
 *
 * <p>Spaces in "public" use the shared test connector. Tests that need a schema of their own (with or without the template, or
 * with a foreign relation of a template name) use a connector with its own ID and schema, so that no other test sees it.</p>
 */
public class PSQLLazyTableIT extends LazyTablesITBase {
  private static final List<String> PUBLIC_SPACES = new ArrayList<>();
  private static final Set<String> SCHEMAS = ConcurrentHashMap.newKeySet();
  private static final Set<String> FUNCTIONS = ConcurrentHashMap.newKeySet();

  @Override
  protected Set<String> schemas() {
    return SCHEMAS;
  }

  @AfterAll
  static void cleanUp() throws Exception {
    invokeDeleteTestSpaces(null, PUBLIC_SPACES);
    for (String function : FUNCTIONS)
      execute(new SQLQuery("DROP FUNCTION IF EXISTS ${schema}.${function}() CASCADE")
          .withVariable("schema", PG_SCHEMA)
          .withVariable("function", function));
    dropSchemas(SCHEMAS);
  }

  private String createPublicSpace() throws Exception {
    String spaceId = newId();
    PUBLIC_SPACES.add(spaceId);
    invokeCreateTestSpace(defaultTestConnectorParams, new Space().withId(spaceId));
    return spaceId;
  }

  private static XyzResponse invokePublic(Event event) throws Exception {
    event.setConnectorParams(defaultTestConnectorParams);
    return XyzSerializable.<XyzResponse>deserialize(invokeLambda(new PSQLXyzConnector(), event));
  }

  private void createGuardTriggers(String table) throws Exception {
    String function = table + "_guard";
    FUNCTIONS.add(function);
    //The same guard as the one of the template
    execute(new SQLQuery("""
        CREATE OR REPLACE FUNCTION ${schema}.${function}() RETURNS trigger LANGUAGE plpgsql AS $guard$
        BEGIN
          RAISE EXCEPTION '${{message}}' USING ERRCODE = 'XYZ52';
        END
        $guard$""")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("function", function)
        .withQueryFragment("message", TEMPLATE_WRITE_MESSAGE));
    for (String guarded : List.of(table, table + "_head", table + "_p0"))
      execute(new SQLQuery("CREATE TRIGGER xyz_test_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON ${schema}.${table} "
          + "FOR EACH STATEMENT EXECUTE FUNCTION ${schema}.${function}()")
          .withVariable("schema", PG_SCHEMA)
          .withVariable("table", guarded)
          .withVariable("function", function));
  }

  //------------ Changes for every space ------------

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void legacyWriterGuardHitIsException(boolean transactional) throws Exception {
    String spaceId = createPublicSpace();
    createGuardTriggers(spaceId);

    ErrorResponse error = assertError(XyzError.EXCEPTION, invokePublic(insertEvent(spaceId, transactional, null)));
    assertEquals(TEMPLATE_WRITE_MESSAGE, error.getErrorMessage());
    assertEquals(0, countRows(PG_SCHEMA, spaceId));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void legacyWriterMissingTableIs404(boolean transactional) throws Exception {
    String spaceId = createPublicSpace();
    //The table set is gone, but the version sequence is still there, so the version is taken and the write itself fails
    dropTableSet(PG_SCHEMA, spaceId);
    execute(new SQLQuery("DROP SEQUENCE IF EXISTS ${schema}.${sequence}")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("sequence", spaceId + "_version_seq"));
    execute(new SQLQuery("CREATE SEQUENCE ${schema}.${sequence}")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("sequence", spaceId + "_version_seq"));

    ErrorResponse error = assertError(XyzError.NOT_FOUND, invokePublic(insertEvent(spaceId, transactional, null)));
    assertEquals("Space not found in backend", error.getErrorMessage());
    assertFalse(relationExists(PG_SCHEMA, spaceId), "Nothing may have been written");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void writeToTheTemplateSequenceIsException(boolean transactional) throws Exception {
    TestConnector connector = newConnector();
    installTemplate(connector.schema());
    Map<String, Object> params = Map.of(Space.TABLE_NAME, TEMPLATE_TABLE);

    ErrorResponse error = assertError(XyzError.EXCEPTION, invoke(connector, insertEvent(newId(), transactional, params)));
    assertEquals(TEMPLATE_WRITE_MESSAGE, error.getErrorMessage());

    error = assertError(XyzError.EXCEPTION, invoke(connector, writeFeaturesEvent(newId(), params)));
    assertEquals(TEMPLATE_WRITE_MESSAGE, error.getErrorMessage());
    assertEquals(0, countRows(connector.schema(), TEMPLATE_TABLE));
  }

  @Test
  public void exhaustedSequenceOfARealSpaceIsNoTemplateWrite() throws Exception {
    String spaceId = createPublicSpace();
    execute(new SQLQuery("ALTER SEQUENCE ${schema}.${sequence} MINVALUE 0 MAXVALUE 1 RESTART WITH 1")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("sequence", spaceId + "_version_seq"));
    query(new SQLQuery("SELECT nextval('${schema}.${sequence}')")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("sequence", spaceId + "_version_seq"), rs -> rs.next());

    ErrorResponse error = assertError(XyzError.EXCEPTION, invokePublic(insertEvent(spaceId, true, null)));
    assertNotEquals(TEMPLATE_WRITE_MESSAGE, error.getErrorMessage());
    error = assertError(XyzError.EXCEPTION, invokePublic(writeFeaturesEvent(spaceId, null)));
    assertNotEquals(TEMPLATE_WRITE_MESSAGE, error.getErrorMessage());
  }

  @Test
  public void deleteNeverDropsReservedName() throws Exception {
    TestConnector connector = newConnector();
    execute(new SQLQuery("CREATE TABLE ${schema}.${table} (id TEXT)")
        .withVariable("schema", connector.schema())
        .withVariable("table", TEMPLATE_TABLE));

    XyzResponse response = invoke(connector, new ModifySpaceEvent()
        .withSpace(newId())
        .withOperation(Operation.DELETE)
        .withParams(Map.of(Space.TABLE_NAME, TEMPLATE_TABLE)));
    assertTrue(response instanceof SuccessResponse, response.serialize());
    assertEquals("OK", ((SuccessResponse) response).getStatus());
    assertTrue(relationExists(connector.schema(), TEMPLATE_TABLE), "The relation with the reserved name must survive");
  }

  @Test
  public void reservedNameCreateIsRejected() throws Exception {
    TestConnector connector = newConnector();
    execute(new SQLQuery("CREATE TABLE ${schema}.${table} (id TEXT)")
        .withVariable("schema", connector.schema())
        .withVariable("table", TEMPLATE_TABLE));
    execute(new SQLQuery("COMMENT ON TABLE ${schema}.${table} IS 'foreign'")
        .withVariable("schema", connector.schema())
        .withVariable("table", TEMPLATE_TABLE));
    Set<String> before = templateRelations(connector.schema());

    String spaceId = newId();
    ErrorResponse error = assertError(XyzError.ILLEGAL_ARGUMENT, invoke(connector, new ModifySpaceEvent()
        .withSpace(spaceId)
        .withOperation(Operation.CREATE)
        .withParams(Map.of(Space.TABLE_NAME, TEMPLATE_TABLE))
        .withSpaceDefinition(new Space().withId(spaceId))));
    assertEquals("Reserved table name", error.getErrorMessage());
    assertEquals(before, templateRelations(connector.schema()), "No relation and no comment may change");
  }

  //------------ The creation of lazy spaces ------------

  @Test
  public void createDoesNoDdl() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String table = newTable();

    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, newId(), lazyParams(table))));
    assertEquals(Set.of(), relations(connector.schema(), table), "A lazy create must not create any relation");
    assertTrue(relationExists(connector.schema(), TEMPLATE_TABLE), "The first event of the connector installs the template");
  }

  @Test
  public void updateBeforeFirstWrite() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String spaceId = newId();
    String table = newTable();
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, spaceId, lazyParams(table))));

    assertOk(invoke(connector, modifySpaceEvent(Operation.UPDATE, spaceId, lazyParams(table))));
    assertEquals(Set.of(), relations(connector.schema(), table));
  }

  @Test
  public void createWithFlagButWithoutConnectorParamIsEager() throws Exception {
    TestConnector connector = newConnector();
    String spaceId = newId();
    String table = newTable();

    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, spaceId, lazyParams(table))));
    assertTrue(relationExists(connector.schema(), table), "The space must be created eagerly");
    assertEquals(1, logLines("WARN Space " + spaceId + ": lazyTable without lazyTableCreation, creating eagerly").size());
  }

  @Test
  public void createWithoutVerifiedTemplateIsEager() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String spaceId = newId();
    String table = newTable();
    PSQLXyzConnector handler = new PSQLXyzConnector();
    handler.initialize(settingsOf(connector, false), null);

    assertOk(invoke(handler, connector, modifySpaceEvent(Operation.CREATE, spaceId, lazyParams(table))));
    assertTrue(relationExists(connector.schema(), table), "The space must be created eagerly");
    assertEquals(1, logLines("WARN Space " + spaceId + ": empty-space template not verified, creating eagerly (template check: null)")
        .size());
    assertFalse(relationExists(connector.schema(), TEMPLATE_TABLE), "Settings without the template install none");
  }

  @Test
  public void newLayoutCreateWithFlagIsEagerAndInstallsNoTemplate() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String table = newTable();

    assertOk(invoke(new NLConnector(), connector, modifySpaceEvent(Operation.CREATE, newId(), lazyParams(table))));
    assertTrue(relationExists(connector.schema(), table), "The new layout creates every space eagerly");
    assertEquals(Set.of(), templateRelations(connector.schema()), "The new layout installs no template");
  }

  @Test
  public void readOnlyConnectorInstallsTemplate() throws Exception {
    TestConnector connector = newConnector().with("readOnly", true);

    XyzResponse response = invoke(connector, new HealthCheckEvent());
    assertFalse(response instanceof ErrorResponse, response.serialize());
    assertTrue(relationExists(connector.schema(), TEMPLATE_TABLE), "A read-only connector installs the template too");
  }

  //------------ Reads of never written lazy spaces ------------

  /**
   * One instance of every read event, as fresh objects, because the connector changes the params of an event.
   */
  private static List<Supplier<Event>> readEvents() {
    return List.of(
        () -> new GetFeaturesByIdEvent().withIds(List.of("F1", "F2")),
        () -> new GetFeaturesByBBoxEvent<>().withBbox(new BBox(-10, -10, 10, 10)),
        () -> new GetFeaturesByTileEvent().withLevel(0).withX(0).withY(0).withBbox(new BBox(-180, -85, 180, 85)),
        () -> new GetFeaturesByGeometryEvent().withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50))).withRadius(1000),
        () -> new IterateFeaturesEvent<>().withLimit(10),
        () -> new SearchForFeaturesEvent<>().withLimit(10),
        () -> new LoadFeaturesEvent().withIdsMap(Map.of("F1", "0")),
        () -> new GetStatisticsEvent().withFastMode(true),
        () -> new GetStatisticsEvent().withFastMode(false),
        () -> new IterateChangesetsEvent().withLimit(10).withRef(new Ref(new Ref(0), new Ref(Ref.HEAD))),
        () -> new GetChangesetStatisticsEvent(),
        () -> new DeleteChangesetsEvent().withMinVersion(0));
  }

  /**
   * @return The response as JSON, without the parts that differ for any two spaces
   */
  private static Map<String, Object> comparable(XyzResponse response) throws Exception {
    Map<String, Object> json = XyzSerializable.deserialize(response.serialize(), new TypeReference<Map<String, Object>>() {});
    json.remove("streamId");
    return json;
  }

  private static void assertParity(TestConnector connector, String eagerSpace, Map<String, Object> eagerParams, String lazySpace,
      Map<String, Object> lazyParams, Supplier<Event> read) throws Exception {
    Event eagerEvent = read.get().withSpace(eagerSpace).withParams(eagerParams);
    Event lazyEvent = read.get().withSpace(lazySpace).withParams(lazyParams);
    String name = eagerEvent.getClass().getSimpleName();
    XyzResponse eager = invoke(connector, eagerEvent);
    XyzResponse lazy = invoke(connector, lazyEvent);
    assertEquals(comparable(eager), comparable(lazy), name + ": " + eager.serialize() + " vs. " + lazy.serialize());
  }

  @Test
  public void readsOfANeverWrittenSpaceEqualThoseOfAnEagerEmptySpace() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String eagerTable = newTable();
    String lazyTable = newTable();
    String spaceId = newId();
    createEager(connector, eagerTable);
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, spaceId, lazyParams(lazyTable))));

    for (Supplier<Event> read : readEvents())
      assertParity(connector, spaceId, eagerParams(eagerTable), spaceId, lazyParams(lazyTable), read);
    assertEquals(Set.of(), relations(connector.schema(), lazyTable), "A read must not create the table");
    //One uncached check per read event, a missing table is never cached
    assertEquals(readEvents().size(), logLines("INFO lazyCheck connector=" + connector.id() + " levels=1 missing=1 ms=").size());
  }

  @Test
  public void eraseIsNoop() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String eagerTable = newTable();
    String lazyTable = newTable();
    createEager(connector, eagerTable);

    XyzResponse eager = invoke(connector, new ModifyFeaturesEvent().withEraseContent(true).withSpace(newId())
        .withParams(eagerParams(eagerTable)));
    XyzResponse lazy = invoke(connector, new ModifyFeaturesEvent().withEraseContent(true).withSpace(newId())
        .withParams(lazyParams(lazyTable)));
    assertEquals(comparable(eager), comparable(lazy));
    assertEquals(Set.of(), relations(connector.schema(), lazyTable));
  }

  /**
   * @return A handler whose settings request no template, so that its schema has none
   */
  private static PSQLXyzConnector handlerWithoutTemplate(TestConnector connector) {
    PSQLXyzConnector handler = new PSQLXyzConnector();
    handler.initialize(settingsOf(connector, false), null);
    return handler;
  }

  @Test
  public void missingTemplateFailsLoudly() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);

    ErrorResponse error = assertError(XyzError.EXCEPTION, invoke(handlerWithoutTemplate(connector), connector,
        new GetFeaturesByIdEvent().withIds(List.of("F1")).withSpace(newId()).withParams(lazyParams(newTable()))));
    assertEquals("Empty-space template missing or not identical on the database", error.getErrorMessage());

    //A write to a lazy extension of a never written lazy base needs the template for the base and fails before any DDL
    String extension = newTable();
    String base = newTable();
    Map<String, Object> params = lazyWriteParams(extension);
    params.put("extends", lazyParams(base));
    ((Map<String, Object>) params.get("extends")).put("spaceId", newId());
    error = assertError(XyzError.EXCEPTION, invoke(handlerWithoutTemplate(connector), connector,
        writeFeaturesEvent(newId(), params)));
    assertEquals("Empty-space template missing or not identical on the database", error.getErrorMessage());
    assertEquals(Set.of(), relations(connector.schema(), extension));
    assertEquals(Set.of(), relations(connector.schema(), base));

    //The first write to a standalone lazy space needs no template
    String standalone = newTable();
    FeatureCollection written = assertFeatureCollection(invoke(handlerWithoutTemplate(connector), connector,
        writeFeaturesEvent(newId(), lazyWriteParams(standalone))));
    assertEquals(Boolean.TRUE, written.getMaterialized());
    assertTrue(relationExists(connector.schema(), standalone));
    assertEquals(Set.of(), templateRelations(connector.schema()));
  }

  //------------ Writes of lazy spaces ------------

  /**
   * The parts of a write response that do not depend on the time or the physical table.
   */
  private static Map<String, Object> writeSummary(XyzResponse response) throws Exception {
    if (response instanceof ErrorResponse error)
      return Map.of("error", String.valueOf(error.getError()));
    FeatureCollection collection = assertFeatureCollection(response);
    return Map.of(
        "features", ids(collection.getFeatures()),
        "oldFeatures", ids(collection.getOldFeatures()),
        "inserted", collection.getInserted() == null ? List.of() : collection.getInserted(),
        "updated", collection.getUpdated() == null ? List.of() : collection.getUpdated(),
        "deleted", collection.getDeleted() == null ? List.of() : collection.getDeleted(),
        "failed", collection.getFailed() == null ? List.of() : collection.getFailed().stream().map(f -> f.getId()).toList());
  }

  private static List<String> ids(List<Feature> features) {
    return features == null ? List.of() : features.stream().map(Feature::getId).sorted().toList();
  }

  /**
   * The writes of the legacy writer and of the feature writer, each on a fresh space.
   */
  private static List<Function<Map<String, Object>, Event>> writes(boolean transactional) {
    return List.of(
        params -> insertEvent("s", transactional, params, "F1", "F2"),
        params -> new ModifyFeaturesEvent().withSpace("s").withParams(params).withTransaction(transactional)
            .withUpsertFeatures(features("U1")),
        params -> new ModifyFeaturesEvent().withSpace("s").withParams(params).withTransaction(transactional)
            .withDeleteFeatures(new HashMap<>(Map.of("D1", "0"))),
        params -> {
          Map<String, Object> withOldStates = new HashMap<>(params);
          withOldStates.put("includeOldStates", true);
          return new ModifyFeaturesEvent().withSpace("s").withParams(withOldStates).withTransaction(transactional)
              .withUpsertFeatures(features("O1"));
        },
        params -> new ModifyFeaturesEvent().withSpace("s").withParams(params).withTransaction(transactional)
            .withInsertFeatures(new ArrayList<>()),
        params -> writeFeaturesEvent("s", params, "W1", "W2"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void firstWritesMaterializeAndAnswerLikeOnAnEagerSpace(boolean transactional) throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    for (Function<Map<String, Object>, Event> write : writes(transactional)) {
      String eagerTable = newTable();
      String lazyTable = newTable();
      createEager(connector, eagerTable);

      XyzResponse eager = invoke(connector, write.apply(eagerParams(eagerTable)));
      Event lazyEvent = write.apply(lazyWriteParams(lazyTable));
      XyzResponse lazy = invoke(connector, lazyEvent);
      String name = lazyEvent.getClass().getSimpleName() + " " + lazy.serialize();
      assertEquals(writeSummary(eager), writeSummary(lazy), name);
      assertEquals(Boolean.TRUE, assertFeatureCollection(lazy).getMaterialized(), name);
      assertEquals(catalogRows(connector.schema(), eagerTable), catalogRows(connector.schema(), lazyTable), name);
    }
  }

  @Test
  public void loadFeaturesAfterFirstWrite() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String table = newTable();
    String spaceId = newId();
    assertFeatureCollection(invoke(connector, insertEvent(spaceId, true, lazyWriteParams(table), "F1")));

    FeatureCollection loaded = assertFeatureCollection(invoke(connector, new LoadFeaturesEvent().withSpace(spaceId)
        .withParams(lazyParams(table)).withIdsMap(new HashMap<>(Map.of("F1", "0")))));
    assertEquals(List.of("F1"), ids(loaded.getFeatures()));
  }

  @Test
  public void materializedSetEqualsEagerCreate() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String eagerTable = newTable();
    String lazyTable = newTable();
    String eagerSpace = newId();
    assertOk(invoke(connector, new ModifySpaceEvent().withSpace(eagerSpace).withOperation(Operation.CREATE)
        .withParams(eagerParams(eagerTable))
        .withSpaceDefinition(new Space().withId(eagerSpace).withSearchableProperties(Map.of("bar.baz", true)))));

    //One on-demand index: the names PostgreSQL gives the partition indexes depend on the order of their creation
    assertFeatureCollection(invoke(connector, writeFeaturesEvent(newId(), lazyWriteParams(lazyTable, "bar.baz"))));
    Set<String> eagerRows = catalogRows(connector.schema(), eagerTable);
    assertTrue(eagerRows.stream().anyMatch(row -> row.contains("'baz'")), "The eager create has the on-demand index: " + eagerRows);
    assertEquals(eagerRows, catalogRows(connector.schema(), lazyTable));
  }

  @Test
  public void writeWithoutOnDemandIndexesMaterializesSystemIndexesOnly() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String eagerTable = newTable();
    String lazyTable = newTable();
    String spaceId = newId();
    createEager(connector, eagerTable);

    FeatureCollection written = assertFeatureCollection(invoke(connector, writeFeaturesEvent(spaceId, lazyParams(lazyTable))));
    assertEquals(Boolean.TRUE, written.getMaterialized());
    assertEquals(catalogRows(connector.schema(), eagerTable), catalogRows(connector.schema(), lazyTable));
    assertEquals(1, logLines("ERROR Space " + spaceId + ": the write carries no onDemandIndexes").size());
  }

  @Test
  public void lostTableOfFlaggedSpaceReadsAsEmpty() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String table = newTable();
    String spaceId = newId();
    assertFeatureCollection(invoke(connector, insertEvent(spaceId, true, lazyWriteParams(table), "F1")));
    dropTableSet(connector.schema(), table);
    //Another container, which has not seen the table
    LazyTableResolver.clearCache();

    FeatureCollection read = assertFeatureCollection(invoke(connector, new GetFeaturesByIdEvent().withSpace(spaceId)
        .withParams(lazyParams(table)).withIds(List.of("F1"))));
    assertEquals(List.of(), ids(read.getFeatures()));

    FeatureCollection written = assertFeatureCollection(invoke(connector, insertEvent(spaceId, true, lazyWriteParams(table), "F2")));
    assertEquals(Boolean.TRUE, written.getMaterialized());
    assertEquals(List.of("F2"), written.getInserted());
    assertTrue(relationExists(connector.schema(), table));
  }

  @Test
  public void writeResponseReportsMaterialized() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);

    //ModifyFeaturesEvent in both modes: CREATED, then a cache hit, then EXISTED in a container without a cache entry
    for (boolean transactional : List.of(true, false)) {
      String table = newTable();
      String spaceId = newId();
      for (String id : List.of("A", "B", "C")) {
        if ("C".equals(id))
          LazyTableResolver.clearCache();
        FeatureCollection written = assertFeatureCollection(invoke(connector,
            insertEvent(spaceId, transactional, lazyWriteParams(table), id)));
        assertEquals(Boolean.TRUE, written.getMaterialized(), "Write of " + id + " transactional " + transactional);
      }
    }

    //WriteFeaturesEvent with and without response data
    for (boolean responseDataExpected : List.of(true, false)) {
      String table = newTable();
      String spaceId = newId();
      for (String id : List.of("A", "B")) {
        FeatureCollection written = assertFeatureCollection(invoke(connector,
            writeFeaturesEvent(spaceId, lazyWriteParams(table), id).withResponseDataExpected(responseDataExpected)));
        assertEquals(Boolean.TRUE, written.getMaterialized(), "Write of " + id + " responseDataExpected " + responseDataExpected);
      }
    }

    //A write with the context SUPER into a flagged base
    String extension = newTable();
    String base = newTable();
    Map<String, Object> params = lazyWriteParams(extension);
    Map<String, Object> baseParams = lazyParams(base);
    baseParams.put("spaceId", newId());
    params.put("extends", baseParams);
    FeatureCollection superWrite = assertFeatureCollection(invoke(connector,
        writeFeaturesEvent(newId(), params, "S1").withContext(SpaceContext.SUPER)));
    assertEquals(Boolean.TRUE, superWrite.getMaterialized());
    assertTrue(relationExists(connector.schema(), base), "The base is the write target");
    assertFalse(relationExists(connector.schema(), extension), "The extension is not written");

    //No report for a space without the flag, a read, an erase and a failed write
    String eagerTable = newTable();
    createEager(connector, eagerTable);
    assertNull(assertFeatureCollection(invoke(connector, writeFeaturesEvent(newId(), eagerParams(eagerTable)))).getMaterialized());
    String table = newTable();
    String spaceId = newId();
    assertFeatureCollection(invoke(connector, writeFeaturesEvent(spaceId, lazyWriteParams(table), "E1")));
    assertNull(assertFeatureCollection(invoke(connector, new GetFeaturesByIdEvent().withSpace(spaceId).withParams(lazyParams(table))
        .withIds(List.of("E1")))).getMaterialized());
    assertNull(assertFeatureCollection(invoke(connector, new ModifyFeaturesEvent().withEraseContent(true).withSpace(spaceId)
        .withParams(lazyParams(table)))).getMaterialized());
    //A transactional update of a feature that does not exist fails the whole write
    XyzResponse conflict = invoke(connector, new ModifyFeaturesEvent().withSpace(spaceId).withParams(lazyWriteParams(table))
        .withTransaction(true).withUpdateFeatures(features("NX")));
    assertError(XyzError.CONFLICT, conflict);
    assertFalse(conflict.serialize().contains("materialized"), conflict.serialize());
  }

  @Test
  public void notIdenticalTemplateFailsLoudly() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    installTemplate(connector.schema());
    execute(new SQLQuery("DROP TRIGGER xyz_empty_space_guard ON ${schema}.${table}")
        .withVariable("schema", connector.schema())
        .withVariable("table", TEMPLATE_TABLE + "_p0"));

    ErrorResponseException e = assertThrows(ErrorResponseException.class, () -> invokeOrThrow(new PSQLXyzConnector(), connector,
        new GetFeaturesByIdEvent().withIds(List.of("F1")).withSpace(newId()).withParams(lazyParams(newTable()))));
    assertEquals(XyzError.EXCEPTION, e.getErrorResponse().getError());
    assertEquals("Empty-space template missing or not identical on the database", e.getErrorResponse().getErrorMessage());
    assertTrue(e.getCause().getMessage().startsWith("Empty-space template not identical: " + TEMPLATE_TABLE + " in schema "),
        e.getCause().getMessage());
  }

  @Test
  public void droppedTemplateStopsLazyCreates() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    String table = newTable();
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, newId(), lazyParams(table))));
    assertEquals(Set.of(), relations(connector.schema(), table));

    dropTableSet(connector.schema(), TEMPLATE_TABLE);
    ErrorResponse error = assertError(XyzError.EXCEPTION, invoke(connector,
        new GetFeaturesByIdEvent().withIds(List.of("F1")).withSpace(newId()).withParams(lazyParams(table))));
    assertEquals("Empty-space template missing or not identical on the database", error.getErrorMessage());

    String spaceId = newId();
    String otherTable = newTable();
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, spaceId, lazyParams(otherTable))));
    assertTrue(relationExists(connector.schema(), otherTable), "Without the template a lazy create is eager");
    assertEquals(1, logLines("WARN Space " + spaceId + ": empty-space template not verified, creating eagerly "
        + "(template check: MISSING_AT_RUNTIME)").size());
    assertEquals(Set.of(), templateRelations(connector.schema()), "Nothing may install the template on the fly");
  }
}
