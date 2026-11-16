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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.execution.StepException;
import com.here.xyz.jobs.steps.impl.SpaceBasedStep.TableState;
import com.here.xyz.jobs.steps.impl.transport.CountSpace;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.jobs.util.test.StepTestBase;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult;
import com.here.xyz.util.db.pg.LazySpaceTables.Profile;
import com.here.xyz.util.db.pg.TemplateUnavailableException;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * The lazy table helpers of {@link SpaceBasedStep} against the local stack: the Hub, the lazy test connectors and their two
 * databases. The helpers are called directly on a step without a space ID, so no step lambda is involved.
 */
@TestInstance(PER_CLASS)
public class SpaceBasedStepLazyIT extends StepTestBase {
  private static final String EAGER_CONNECTOR = "psql";
  private static final String LAZY_CONNECTOR = "psql-lazy";
  private static final String LAZY_DB2_CONNECTOR = "psql-db2-lazy";
  private static final String PG_HOST = System.getProperty("pg.host", "localhost");
  private static final String DB_URL = "jdbc:postgresql://" + PG_HOST + "/postgres";
  private static final String DB2_URL = "jdbc:postgresql://" + PG_HOST + "/postgres_db2";
  private static final String DB_USER = "postgres";
  private static final String DB_PASSWORD = "password";
  private static final String PUBLIC = "public";

  private final List<String> createdSpaces = new ArrayList<>();
  private final List<String> createdConnectors = new ArrayList<>();
  private final List<String> createdSchemas = new ArrayList<>();
  private int spaceCounter;

  @AfterAll
  public void tearDown() throws SQLException {
    //Extensions were created after their bases, so they are removed first
    for (int i = createdSpaces.size() - 1; i >= 0; i--)
      deleteSpace(createdSpaces.get(i));
    for (String connectorId : createdConnectors)
      deleteConnector(connectorId);
    for (String schema : createdSchemas)
      execute(DB_URL, "DROP SCHEMA IF EXISTS " + quote(schema) + " CASCADE");
  }

  @Test
  public void materializationUsesTheDatabaseOfEachSpace() throws Exception {
    Space lazy = createSpaceOn(LAZY_CONNECTOR);
    Space lazyOnDb2 = createSpaceOn(LAZY_DB2_CONNECTOR);
    HelperStep step = new HelperStep();

    assertEquals(MaterializationResult.CREATED, step.materializeTables(step.load(lazy.getId()), Profile.FULL));
    assertEquals(MaterializationResult.CREATED, step.materializeTables(step.load(lazyOnDb2.getId()), Profile.FULL));

    String table = tableName(lazy), tableOnDb2 = tableName(lazyOnDb2);
    assertTrue(hasRelation(DB_URL, PUBLIC, table));
    assertEquals(0, relationCount(DB2_URL, table));
    assertTrue(hasRelation(DB2_URL, PUBLIC, tableOnDb2));
    assertEquals(0, relationCount(DB_URL, tableOnDb2));

    //A materialization alone keeps the flag
    assertEquals(0, step.clearRequests.get());
    assertTrue(isFlagged(lazy.getId()));
    assertTrue(isFlagged(lazyOnDb2.getId()));

    //A later invocation checks again
    HelperStep laterStep = new HelperStep();
    assertEquals(MaterializationResult.EXISTED, laterStep.materializeTables(laterStep.load(lazyOnDb2.getId()), Profile.FULL));
    assertEquals(TableState.EXISTS, laterStep.tableState(laterStep.load(lazyOnDb2.getId())));
  }

  @Test
  public void neverWrittenLazySpaceOnTheSecondDatabaseReadsTheTemplate() throws Exception {
    Space lazyOnDb2 = createSpaceOn(LAZY_DB2_CONNECTOR);
    HelperStep step = new HelperStep();
    Space snapshot = step.load(lazyOnDb2.getId());

    assertTrue(hasRelation(DB2_URL, PUBLIC, TEMPLATE_TABLE));
    assertEquals(TableState.NEVER_MATERIALIZED, step.tableState(snapshot));
    assertEquals(TEMPLATE_TABLE, step.getReadableRootTableName(snapshot));
    assertEquals(TEMPLATE_TABLE, step.getReadableRootTableNameOrFail(snapshot));
    Map<String, Object> params = step.resolveReadableSpaceParams(snapshot);
    assertEquals(TEMPLATE_TABLE, params.get(TABLE_NAME));
    //The step's snapshot keeps its own table name
    assertEquals(tableName(lazyOnDb2), snapshot.getStorage().getParams().get(TABLE_NAME));
    assertEquals(0, relationCount(DB2_URL, tableName(lazyOnDb2)));
  }

  @Test
  public void tableStateIsTheStateOfTheOwnTable() throws Exception {
    Space eager = createSpaceOn(EAGER_CONNECTOR);
    Space lazyBase = createSpaceOn(LAZY_CONNECTOR);
    Space lazyExtension = createExtension(lazyBase.getId());
    HelperStep step = new HelperStep();

    assertEquals(TableState.NOT_LAZY, step.tableState(step.load(eager.getId())));
    assertEquals(TableState.NEVER_MATERIALIZED, step.tableState(step.load(lazyBase.getId())));
    assertTrue(step.isLazy(step.load(lazyExtension.getId())));
    assertEquals(TableState.NEVER_MATERIALIZED, step.tableState(step.load(lazyExtension.getId())));
    assertEquals(MaterializationResult.CREATED, step.materializeTables(step.load(lazyExtension.getId()), Profile.FULL));

    HelperStep laterStep = new HelperStep();
    Space extension = laterStep.load(lazyExtension.getId());
    assertEquals(TableState.EXISTS, laterStep.tableState(extension));
    assertEquals(TableState.NEVER_MATERIALIZED, laterStep.tableState(laterStep.load(lazyBase.getId())));
    assertEquals(tableName(lazyExtension), laterStep.getReadableRootTableName(extension));
    Map<String, Object> params = laterStep.resolveReadableSpaceParams(extension);
    assertEquals(tableName(lazyExtension), params.get(TABLE_NAME));
    assertEquals(TEMPLATE_TABLE, extendsParams(params).get(TABLE_NAME));
  }

  @Test
  public void superSpaceOnAnotherDatabaseIsNotChecked() throws Exception {
    Space lazyBase = createSpaceOn(LAZY_CONNECTOR);
    Space extension = createExtension(lazyBase.getId());
    sendHubRequest("PATCH", "/spaces/" + extension.getId() + "?forceStorage=true",
        XyzSerializable.serialize(Map.of("storage", Map.of("id", LAZY_DB2_CONNECTOR))));
    HelperStep step = new HelperStep();
    Space extensionSnapshot = step.load(extension.getId());
    Space baseSnapshot = step.load(lazyBase.getId());
    assertEquals(LAZY_DB2_CONNECTOR, extensionSnapshot.getStorage().getId());

    //The base is on another storage than the reader, so it is read by its physical name, as an eager base would be
    assertEquals(tableName(lazyBase), step.getReadableRootTableName(baseSnapshot, extensionSnapshot));
    assertEquals(tableName(lazyBase), step.getReadableRootTableNameOrFail(baseSnapshot, extensionSnapshot));
    Map<String, Object> params = step.resolveReadableSpaceParams(baseSnapshot, extensionSnapshot);
    assertEquals(tableName(lazyBase), params.get(TABLE_NAME));
    assertFalse(params.containsKey(LAZY_TABLE));
    //The extension's composite params carry no flag for a base with another storage ID
    assertFalse(extendsParams(step.resolveSpaceParams(extensionSnapshot)).containsKey(LAZY_TABLE));

    //On its own database the never written base reads the template
    assertEquals(TEMPLATE_TABLE, step.getReadableRootTableName(baseSnapshot));
  }

  @Test
  public void missingTemplateFailsNonRetryable() throws Exception {
    String schema = ("lazy_" + randomAlpha(5)).toLowerCase();
    createdSchemas.add(schema);
    execute(DB_URL, "CREATE SCHEMA " + quote(schema));
    String connectorId = getClass().getSimpleName() + "_" + randomAlpha(5);
    createdConnectors.add(connectorId);
    createConnectorCopy(LAZY_CONNECTOR, connectorId, Map.of("ecps", ecpsWithSchema(LAZY_CONNECTOR, schema)));

    /*
    Only the connector installs the template, and no event ever reaches this connector, so its schema has none. The steps
    need only the connector's config, so the space exists only as the step's snapshot.
     */
    String table = ("lazy_missing_" + randomAlpha(8)).toLowerCase();
    Space snapshot = new Space().withId(nextSpaceId()).withStorage(new ConnectorRef().withId(connectorId)
        .withParams(new HashMap<>(Map.of(TABLE_NAME, table, LAZY_TABLE, true))));
    HelperStep step = new HelperStep();
    step.webClient.knownSpaces.put(snapshot.getId(), snapshot);
    assertFalse(hasRelation(DB_URL, schema, TEMPLATE_TABLE));

    TemplateUnavailableException missing = assertThrows(TemplateUnavailableException.class,
        () -> step.getReadableRootTableName(snapshot));
    assertTrue(missing.getMessage().startsWith("Empty-space template missing"));
    StepException stepException = assertThrows(StepException.class, () -> step.getReadableRootTableNameOrFail(snapshot));
    assertFalse(stepException.isRetryable());
    assertInstanceOf(TemplateUnavailableException.class, stepException.getCause());
    assertTrue(stepException.getCause().getMessage().startsWith("Empty-space template missing"));
    StepException paramsException = assertThrows(StepException.class, () -> step.resolveReadableSpaceParamsOrFail(snapshot));
    assertFalse(paramsException.isRetryable());

    //A standalone lazy space substitutes nothing, so its materialization needs no template
    assertEquals(MaterializationResult.CREATED, step.materializeTables(snapshot, Profile.FULL));
    assertTrue(hasRelation(DB_URL, schema, table));
    assertFalse(hasRelation(DB_URL, PUBLIC, table));
    assertEquals(TableState.EXISTS, step.tableState(snapshot));
    assertEquals(table, step.getReadableRootTableNameOrFail(snapshot));
  }

  private Space createSpaceOn(String connectorId) {
    Space space = createSpace(new Space().withId(nextSpaceId()).withStorage(new ConnectorRef().withId(connectorId)), false);
    assertNotNull(space, "The space on " + connectorId + " was not created");
    createdSpaces.add(space.getId());
    return loadSpace(space.getId());
  }

  private Space createExtension(String baseId) {
    Space space = createSpace(new Space().withId(nextSpaceId()).withExtension(new Extension().withSpaceId(baseId)), false);
    assertNotNull(space, "The extension of " + baseId + " was not created");
    createdSpaces.add(space.getId());
    return loadSpace(space.getId());
  }

  private String nextSpaceId() {
    return SPACE_ID + "-" + ++spaceCounter;
  }

  private boolean isFlagged(String spaceId) {
    return Space.isLazyTable(loadSpace(spaceId).getStorage().getParams());
  }

  private static String tableName(Space space) {
    return (String) space.getStorage().getParams().get(TABLE_NAME);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> extendsParams(Map<String, Object> params) {
    return (Map<String, Object>) params.get("extends");
  }

  private static String ecpsWithSchema(String connectorId, String schema) throws Exception {
    String ecps = (String) HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadConnector(connectorId).params.get("ecps");
    Map<String, Object> settings = new HashMap<>(ECPSTool.decryptToMap(Config.instance.ECPS_PHRASE, ecps));
    settings.put("PSQL_SCHEMA", schema);
    return ECPSTool.encrypt(Config.instance.ECPS_PHRASE, XyzSerializable.serialize(settings));
  }

  private static Connection connect(String url) throws SQLException {
    return DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
  }

  private static void execute(String url, String statement) throws SQLException {
    try (Connection connection = connect(url); Statement stmt = connection.createStatement()) {
      stmt.execute(statement);
    }
  }

  private static boolean hasRelation(String url, String schema, String name) throws SQLException {
    return count(url, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? "
        + "AND c.relname = ?", schema, name) > 0;
  }

  /**
   * @return The number of relations whose name starts with the table name: the table, its partitions, sequences and indexes
   */
  private static int relationCount(String url, String table) throws SQLException {
    return count(url, "SELECT count(*) FROM pg_class WHERE starts_with(relname, ?)", table);
  }

  private static int count(String url, String query, String... values) throws SQLException {
    try (Connection connection = connect(url); PreparedStatement stmt = connection.prepareStatement(query)) {
      for (int i = 0; i < values.length; i++)
        stmt.setString(i + 1, values[i]);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  /**
   * A step without a space ID that calls the helpers for the spaces it gets, as MapFlow's event import does. It counts the
   * clear requests to the Hub.
   */
  private class HelperStep extends CountSpace {
    private final AtomicInteger clearRequests = new AtomicInteger();
    private final TestHubWebClient webClient = new TestHubWebClient(clearRequests);

    private HelperStep() {
      withJobId(JOB_ID);
    }

    private Space load(String spaceId) throws WebClientException {
      return space(spaceId);
    }

    @Override
    protected HubWebClient hubWebClient() {
      return webClient;
    }

    @Override
    public List<Load> getNeededResources() {
      return List.of();
    }
  }

  /**
   * The Hub client of the local stack, which counts the clear requests and answers for spaces that exist only in the test.
   */
  private static class TestHubWebClient extends HubWebClient {
    private final AtomicInteger clearRequests;
    private final Map<String, Space> knownSpaces = new HashMap<>();

    private TestHubWebClient(AtomicInteger clearRequests) {
      super(Config.instance.HUB_ENDPOINT);
      this.clearRequests = clearRequests;
    }

    @Override
    public Space loadSpace(String spaceId, boolean skipLocalCache) throws WebClientException {
      Space known = knownSpaces.get(spaceId);
      return known != null ? known : super.loadSpace(spaceId, skipLocalCache);
    }

    @Override
    public void clearLazyTable(String spaceId, String connectorId, String tableName) throws WebClientException {
      clearRequests.incrementAndGet();
      super.clearLazyTable(spaceId, connectorId, tableName);
    }
  }
}
