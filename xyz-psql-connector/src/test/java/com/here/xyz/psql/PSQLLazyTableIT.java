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

import static com.here.xyz.psql.DatabaseHandler.ECPS_PHRASE;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_HOST;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_PASSWORD;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_SCHEMA;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_USER;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_WRITE_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.pg.ScriptInstaller.TemplateCheck;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.dbutils.ResultSetHandler;
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
public class PSQLLazyTableIT extends PSQLAbstractIT {
  private static final String ECPS_PASSPHRASE = "testing";
  private static final SimulatedContext CONTEXT = new SimulatedContext("xyz-psql-local", Map.of(ECPS_PHRASE, ECPS_PASSPHRASE));
  private static final List<String> PUBLIC_SPACES = new ArrayList<>();
  private static final Set<String> SCHEMAS = ConcurrentHashMap.newKeySet();
  private static final Set<String> FUNCTIONS = ConcurrentHashMap.newKeySet();

  /**
   * A connector with its own ID and schema.
   */
  record TestConnector(String id, String schema, Map<String, Object> params) {
    TestConnector with(String key, Object value) {
      Map<String, Object> params = new HashMap<>(params());
      params.put(key, value);
      return new TestConnector(id, schema, params);
    }

    Map<String, Object> connectorParams() throws GeneralSecurityException {
      Map<String, Object> connectorParams = new HashMap<>(params);
      connectorParams.put(CONNECTOR_ID, id);
      connectorParams.put("ecps", ECPSTool.encrypt(ECPS_PASSPHRASE, XyzSerializable.serialize(Map.of(
          PSQL_HOST, PG_HOST,
          PSQL_USER, PG_USER,
          PSQL_PASSWORD, PG_PW,
          PSQL_SCHEMA, schema))));
      return connectorParams;
    }
  }

  @AfterAll
  static void cleanUp() throws Exception {
    invokeDeleteTestSpaces(null, PUBLIC_SPACES);
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      for (String function : FUNCTIONS)
        new SQLQuery("DROP FUNCTION IF EXISTS ${schema}.${function}() CASCADE")
            .withVariable("schema", PG_SCHEMA)
            .withVariable("function", function)
            .write(dsp);
      for (String schema : SCHEMAS)
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
    }
  }

  private String newId() {
    return getClass().getSimpleName() + "_" + Random.randomAlpha(5);
  }

  /**
   * @return A connector with its own ID and a schema of its own, which is created and dropped in {@link #cleanUp()}
   */
  private TestConnector newConnector() throws Exception {
    String schema = "lazy_" + Random.randomAlpha(5).toLowerCase();
    SCHEMAS.add(schema);
    execute(new SQLQuery("CREATE SCHEMA IF NOT EXISTS ${schema}").withVariable("schema", schema));
    return new TestConnector(newId(), schema, Map.of("propertySearch", true));
  }

  private String createPublicSpace() throws Exception {
    String spaceId = newId();
    PUBLIC_SPACES.add(spaceId);
    invokeCreateTestSpace(defaultTestConnectorParams, new Space().withId(spaceId));
    return spaceId;
  }

  /**
   * Invokes a new connector instance, as Lambda does for every event.
   */
  private static XyzResponse invoke(TestConnector connector, Event event) throws Exception {
    event.setConnectorParams(connector.connectorParams());
    new LambdaFunctionRuntime(CONTEXT, "lazy-" + Random.randomAlpha(5));
    try {
      return (XyzResponse) new PSQLXyzConnector().handleEvent(event);
    }
    catch (ErrorResponseException e) {
      return e.getErrorResponse();
    }
  }

  private static XyzResponse invokePublic(Event event) throws Exception {
    event.setConnectorParams(defaultTestConnectorParams);
    return XyzSerializable.<XyzResponse>deserialize(invokeLambda(new PSQLXyzConnector(), event));
  }

  /**
   * Installs the template into the schema the way the connectors do: with the script check of a new data source provider.
   */
  private static void installTemplate(String schema) throws Exception {
    DatabaseSettings settings = new DatabaseSettings(PSQLLazyTableIT.class.getSimpleName() + "_" + Random.randomAlpha(5))
        .withApplicationName(PSQLLazyTableIT.class.getSimpleName())
        .withHost(PG_HOST)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(PG_PW)
        .withSchema(schema)
        .withDbMaxPoolSize(1)
        .withEmptySpaceTemplate(true)
        .withScriptResourcePaths(List.of(new ScriptResourcePath("/sql", "hub", "common")));
    try (PooledDataSources ignored = new PooledDataSources(settings)) {
      assertEquals(TemplateCheck.VERIFIED, settings.getEmptySpaceTemplateCheck());
    }
  }

  private static void execute(SQLQuery query) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      query.write(dsp);
    }
  }

  private static <R> R query(SQLQuery query, ResultSetHandler<R> handler) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return query.run(dsp, handler);
    }
  }

  private static boolean relationExists(String schema, String relation) throws Exception {
    return query(new SQLQuery("SELECT to_regclass(#{name}) IS NOT NULL")
        .withNamedParameter("name", "\"" + schema + "\".\"" + relation + "\""), rs -> rs.next() && rs.getBoolean(1));
  }

  private static long countRows(String schema, String table) throws Exception {
    return query(new SQLQuery("SELECT count(*) FROM ${schema}.${table}")
        .withVariable("schema", schema)
        .withVariable("table", table), rs -> rs.next() ? rs.getLong(1) : -1);
  }

  /**
   * @return The relations of the schema whose names start with the prefix, each with its kind and comment
   */
  private static Set<String> relations(String schema, String prefix) throws Exception {
    return query(new SQLQuery("""
        SELECT c.relname || ':' || c.relkind::TEXT || ':' || coalesce(obj_description(c.oid, 'pg_class'), '') FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = #{schema} AND starts_with(c.relname, #{prefix})
        """)
        .withNamedParameter("schema", schema)
        .withNamedParameter("prefix", prefix), rs -> {
          Set<String> rows = new TreeSet<>();
          while (rs.next())
            rows.add(rs.getString(1));
          return rows;
        });
  }

  private static List<Feature> features(String... ids) {
    //A mutable list, because the connector adds to the lists of the event
    List<Feature> features = new ArrayList<>();
    for (String id : ids)
      features.add(new Feature().withId(id)
          .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)))
          .withProperties(new Properties().withXyzNamespace(new XyzNamespace()).with("value", id)));
    return features;
  }

  private static ModifyFeaturesEvent insertEvent(String spaceId, boolean transactional, Map<String, Object> params) {
    return new ModifyFeaturesEvent()
        .withSpace(spaceId)
        .withParams(params)
        .withTransaction(transactional)
        .withInsertFeatures(features("F1", "F2"));
  }

  private static WriteFeaturesEvent writeFeaturesEvent(String spaceId, Map<String, Object> params) {
    return new WriteFeaturesEvent()
        .withSpace(spaceId)
        .withParams(params)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new Modification()
            .withFeatureData(new FeatureCollection().withFeatures(features("W1")))
            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)));
  }

  private static ErrorResponse assertError(XyzError expected, XyzResponse response) throws Exception {
    assertTrue(response instanceof ErrorResponse, "Expected an error response: " + response.serialize());
    ErrorResponse error = (ErrorResponse) response;
    assertEquals(expected, error.getError(), error.serialize());
    return error;
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
    execute(new SQLQuery("DROP TABLE ${schema}.${table} CASCADE").withVariable("schema", PG_SCHEMA).withVariable("table", spaceId));
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
    Set<String> before = relations(connector.schema(), "xyz_empty_space_");

    String spaceId = newId();
    ErrorResponse error = assertError(XyzError.ILLEGAL_ARGUMENT, invoke(connector, new ModifySpaceEvent()
        .withSpace(spaceId)
        .withOperation(Operation.CREATE)
        .withParams(Map.of(Space.TABLE_NAME, TEMPLATE_TABLE))
        .withSpaceDefinition(new Space().withId(spaceId))));
    assertEquals("Reserved table name", error.getErrorMessage());
    assertEquals(before, relations(connector.schema(), "xyz_empty_space_"), "No relation and no comment may change");
  }
}
