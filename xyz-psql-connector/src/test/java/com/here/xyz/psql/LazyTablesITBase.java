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
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.here.xyz.psql.query.ModifySpace;
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
import com.here.xyz.util.db.pg.LazySpaceTables;
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
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * Helpers of the connector tests of lazy space tables. Each test that needs a template state of its own uses a connector with
 * its own ID and schema, so that no other test sees it. The subclasses drop their schemas in their own clean-up.
 */
public abstract class LazyTablesITBase extends PSQLAbstractIT {
  private static final String ECPS_PASSPHRASE = "testing";
  private static final SimulatedContext CONTEXT = new SimulatedContext("xyz-psql-local", Map.of(ECPS_PHRASE, ECPS_PASSPHRASE));
  //The log lines of the classes under test, each test filters them by its own space or table
  protected static final CapturingAppender APPENDER = new CapturingAppender();
  private static final List<String> CAPTURED_LOGGERS = List.of(ModifySpace.class.getName(), LazyTableResolver.class.getName(),
      DatabaseHandler.class.getName());

  static {
    APPENDER.start();
    for (String logger : CAPTURED_LOGGERS)
      ((org.apache.logging.log4j.core.Logger) LogManager.getLogger(logger)).addAppender(APPENDER);
  }

  /**
   * Collects the formatted messages of INFO and above.
   */
  protected static class CapturingAppender extends AbstractAppender {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    CapturingAppender() {
      super(LazyTablesITBase.class.getSimpleName() + "-" + Random.randomAlpha(5), null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel().isMoreSpecificThan(Level.INFO))
        messages.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
    }

    List<String> messages(String part) {
      return messages.stream().filter(message -> message.contains(part)).toList();
    }
  }

  /**
   * A connector with its own ID and schema.
   */
  protected record TestConnector(String id, String schema, Map<String, Object> params) {
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

  /**
   * @return The schemas this test class created, which its clean-up drops
   */
  protected abstract Set<String> schemas();

  protected static void dropSchemas(Set<String> schemas) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      for (String schema : schemas)
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
    }
  }

  protected String newId() {
    return getClass().getSimpleName() + "_" + Random.randomAlpha(5);
  }

  protected static String newTable() {
    return "lazy_t_" + Random.randomAlpha(8).toLowerCase();
  }

  /**
   * @return A connector with its own ID and a schema of its own
   */
  protected TestConnector newConnector() throws Exception {
    String schema = "lazy_" + Random.randomAlpha(5).toLowerCase();
    schemas().add(schema);
    execute(new SQLQuery("CREATE SCHEMA IF NOT EXISTS ${schema}").withVariable("schema", schema));
    return new TestConnector(newId(), schema, Map.of("propertySearch", true));
  }

  /**
   * Invokes a new connector instance, as Lambda does for every event.
   */
  protected static XyzResponse invoke(TestConnector connector, Event event) throws Exception {
    return invoke(new PSQLXyzConnector(), connector, event);
  }

  protected static XyzResponse invoke(DatabaseHandler handler, TestConnector connector, Event event) throws Exception {
    try {
      return invokeOrThrow(handler, connector, event);
    }
    catch (ErrorResponseException e) {
      return e.getErrorResponse();
    }
  }

  /**
   * Like {@link #invoke(DatabaseHandler, TestConnector, Event)}, but throws the {@link ErrorResponseException} with its cause.
   */
  protected static XyzResponse invokeOrThrow(DatabaseHandler handler, TestConnector connector, Event event) throws Exception {
    event.setConnectorParams(connector.connectorParams());
    new LambdaFunctionRuntime(CONTEXT, "lazy-" + Random.randomAlpha(5));
    return (XyzResponse) handler.handleEvent(event);
  }

  /**
   * @return Settings for the schema of the connector, as embedded setups and tests pass them to
   *  {@link DatabaseHandler#initialize(DatabaseSettings, com.amazonaws.services.lambda.runtime.Context)}
   */
  protected static DatabaseSettings settingsOf(TestConnector connector, boolean emptySpaceTemplate) {
    return new DatabaseSettings(connector.id())
        .withApplicationName(LazyTablesITBase.class.getSimpleName())
        .withHost(PG_HOST)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(PG_PW)
        .withSchema(connector.schema())
        .withDbMaxPoolSize(1)
        .withEmptySpaceTemplate(emptySpaceTemplate)
        .withScriptResourcePaths(List.of(new ScriptResourcePath("/sql", "hub", "common")));
  }

  /**
   * Installs the template into the schema the way the connectors do: with the script check of a new data source provider.
   */
  protected static void installTemplate(String schema) throws Exception {
    DatabaseSettings settings = new DatabaseSettings(LazyTablesITBase.class.getSimpleName() + "_" + Random.randomAlpha(5))
        .withApplicationName(LazyTablesITBase.class.getSimpleName())
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

  protected static ModifySpaceEvent modifySpaceEvent(Operation operation, String spaceId, Map<String, Object> params) {
    return new ModifySpaceEvent()
        .withSpace(spaceId)
        .withOperation(operation)
        .withParams(params)
        .withSpaceDefinition(new Space().withId(spaceId));
  }

  protected static Map<String, Object> lazyParams(String table) {
    return new HashMap<>(Map.of(Space.TABLE_NAME, table, Space.LAZY_TABLE, true));
  }

  protected static Map<String, Object> eagerParams(String table) {
    return new HashMap<>(Map.of(Space.TABLE_NAME, table));
  }

  /**
   * Creates the table of a space eagerly through the connector.
   */
  protected void createEager(TestConnector connector, String table) throws Exception {
    assertOk(invoke(connector, modifySpaceEvent(Operation.CREATE, newId(), eagerParams(table))));
  }

  protected static void assertOk(XyzResponse response) throws Exception {
    assertTrue(response instanceof SuccessResponse, "Expected a success response: " + response.serialize());
    assertEquals("OK", ((SuccessResponse) response).getStatus());
  }

  protected static ErrorResponse assertError(XyzError expected, XyzResponse response) throws Exception {
    assertTrue(response instanceof ErrorResponse, "Expected an error response: " + response.serialize());
    ErrorResponse error = (ErrorResponse) response;
    assertEquals(expected, error.getError(), error.serialize());
    return error;
  }

  protected static FeatureCollection assertFeatureCollection(XyzResponse response) throws Exception {
    assertTrue(response instanceof FeatureCollection, "Expected a feature collection: " + response.serialize());
    return (FeatureCollection) response;
  }

  protected static List<String> logLines(String part) {
    return APPENDER.messages(part);
  }

  protected static void execute(SQLQuery query) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      query.write(dsp);
    }
  }

  protected static <R> R query(SQLQuery query, ResultSetHandler<R> handler) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return query.run(dsp, handler);
    }
  }

  protected static boolean relationExists(String schema, String relation) throws Exception {
    return query(new SQLQuery("SELECT to_regclass(#{name}) IS NOT NULL")
        .withNamedParameter("name", "\"" + schema + "\".\"" + relation + "\""), rs -> rs.next() && rs.getBoolean(1));
  }

  protected static long countRows(String schema, String table) throws Exception {
    return query(new SQLQuery("SELECT count(*) FROM ${schema}.${table}")
        .withVariable("schema", schema)
        .withVariable("table", table), rs -> rs.next() ? rs.getLong(1) : -1);
  }

  protected static void dropTableSet(String schema, String table) throws Exception {
    execute(new SQLQuery("DROP TABLE ${schema}.${table} CASCADE").withVariable("schema", schema).withVariable("table", table));
  }

  /**
   * @return The relations of the schema whose names start with the prefix, each with its kind and comment
   */
  protected static Set<String> relations(String schema, String prefix) throws Exception {
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

  protected static Set<String> templateRelations(String schema) throws Exception {
    return relations(schema, LazySpaceTables.RESERVED_PREFIX);
  }

  protected static List<Feature> features(String... ids) {
    //A mutable list, because the connector adds to the lists of the event
    List<Feature> features = new ArrayList<>();
    for (String id : ids)
      features.add(new Feature().withId(id)
          .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50)))
          .withProperties(new Properties().withXyzNamespace(new XyzNamespace()).with("value", id)));
    return features;
  }

  protected static ModifyFeaturesEvent insertEvent(String spaceId, boolean transactional, Map<String, Object> params) {
    return insertEvent(spaceId, transactional, params, "F1", "F2");
  }

  protected static ModifyFeaturesEvent insertEvent(String spaceId, boolean transactional, Map<String, Object> params,
      String... ids) {
    return new ModifyFeaturesEvent()
        .withSpace(spaceId)
        .withParams(params)
        .withTransaction(transactional)
        .withInsertFeatures(features(ids));
  }

  protected static WriteFeaturesEvent writeFeaturesEvent(String spaceId, Map<String, Object> params, String... ids) {
    return new WriteFeaturesEvent()
        .withSpace(spaceId)
        .withParams(params)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new Modification()
            .withFeatureData(new FeatureCollection().withFeatures(features(ids.length == 0 ? new String[]{"W1"} : ids)))
            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)));
  }

  /**
   * Writes features into a table as a space of its own.
   */
  protected static void insertInto(TestConnector connector, String table, String... ids) throws Exception {
    FeatureCollection result = assertFeatureCollection(invoke(connector, insertEvent(table, true, eagerParams(table), ids)));
    assertEquals(ids.length, result.getInserted() == null ? 0 : result.getInserted().size(), result.serialize());
  }
}
