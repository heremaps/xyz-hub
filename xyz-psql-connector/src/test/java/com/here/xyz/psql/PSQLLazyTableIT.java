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
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.db.SQLQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

  @Test
  public void missingTemplateFailsLoudly() throws Exception {
    TestConnector connector = newConnector().with("lazyTableCreation", true);
    PSQLXyzConnector handler = new PSQLXyzConnector();
    handler.initialize(settingsOf(connector, false), null);

    ErrorResponse error = assertError(XyzError.EXCEPTION, invoke(handler, connector,
        new GetFeaturesByIdEvent().withIds(List.of("F1")).withSpace(newId()).withParams(lazyParams(newTable()))));
    assertEquals("Empty-space template missing or not identical on the database", error.getErrorMessage());
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
