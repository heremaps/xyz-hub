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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.FeatureCollection.ModificationFailure;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Writes after a version jump past a missing history partition (DS-2339). The version sequence is moved to 300010, so the
 * history rows of an update belong into the partition "_p3", which does not exist, and the update hits the check violation
 * "no partition of relation". The connector has to create the partition and retry the write within the same request.
 *
 * <p>Like Lambda, every request uses a new connector instance with the default pool size of 1. The time limit is well below
 * the connection checkout timeout (7 s), the lock timeout (16 s) and the statement timeout (23 s).</p>
 */
public class PSQLHistoryPartitionRecoveryIT extends PSQLAbstractIT {

  private static final long VERSION_AFTER_JUMP = 300_010;
  private static final long MAX_REQUEST_MILLIS = 5_000;
  private static final List<String> SPACE_IDS = new ArrayList<>();
  private static final List<String> FEATURE_IDS = List.of("F1", "F2", "F3");

  private String spaceId;

  @BeforeEach
  void createSpaceAndJumpVersion() throws Exception {
    spaceId = getClass().getSimpleName() + "_" + Random.randomAlpha(5);
    SPACE_IDS.add(spaceId);
    invokeCreateTestSpace(defaultTestConnectorParams, new Space().withId(spaceId).withVersionsToKeep(1000));

    FeatureCollection inserted = deserializeResponse(invokeLambda(new PSQLXyzConnector(), new ModifyFeaturesEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(spaceId)
        .withVersionsToKeep(1000)
        .withTransaction(true)
        .withInsertFeatures(features(FEATURE_IDS, "initial"))));
    assertEquals(Set.copyOf(FEATURE_IDS), Set.copyOf(inserted.getInserted()));

    assertFalse(tableExists(partition3()), "The partition _p3 must not exist before the version jump");
    query(new SQLQuery("SELECT setval('${schema}.${sequence}', #{value})")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("sequence", spaceId + "_version_seq")
        .withNamedParameter("value", VERSION_AFTER_JUMP - 1), rs -> rs.next());
  }

  @AfterAll
  static void deleteSpaces() throws Exception {
    invokeDeleteTestSpaces(null, SPACE_IDS);
    //The space deletion does not remove an unattached "_p3" table
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      for (String spaceId : SPACE_IDS)
        new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table}")
            .withVariable("schema", PG_SCHEMA)
            .withVariable("table", spaceId + "_p3")
            .write(dsp);
    }
  }

  @Test
  void transactionalUpdateAfterVersionJumpSucceeds() throws Exception {
    PSQLXyzConnector connector = new PSQLXyzConnector();
    TimedResponse update = invoke(connector, updateEvent(true, FEATURE_IDS));
    assertEquals(1, connector.getDatabaseSettings().getDbMaxPoolSize(), "The test must run with the Lambda pool size of 1");

    FeatureCollection result = deserializeResponse(update.response());
    assertEquals(Set.copyOf(FEATURE_IDS), Set.copyOf(result.getUpdated()));
    assertTrue(result.getFailed() == null || result.getFailed().isEmpty(), "Unexpected failures: " + update.response());
    assertFasterThanLimit(update);
    assertTrue(isAttachedPartition(partition3()), "The partition _p3 must have been created and attached");
    assertEquals(3, countRows(partition3()));

    TimedResponse nextUpdate = invoke(new PSQLXyzConnector(), updateEvent(true, FEATURE_IDS));
    FeatureCollection nextResult = deserializeResponse(nextUpdate.response());
    assertEquals(Set.copyOf(FEATURE_IDS), Set.copyOf(nextResult.getUpdated()));
    assertFasterThanLimit(nextUpdate);
  }

  @Test
  void nonTransactionalUpdateAfterVersionJumpSucceeds() throws Exception {
    //NX does not exist, so its update fails before F1 hits the missing partition. The retry must not report it twice.
    TimedResponse update = invoke(new PSQLXyzConnector(), updateEvent(false, List.of("NX", "F1", "F2", "F3")));

    FeatureCollection result = deserializeResponse(update.response());
    assertEquals(Set.copyOf(FEATURE_IDS), Set.copyOf(result.getUpdated()), "Unexpected result: " + update.response());
    assertEquals(List.of("NX"), failedIds(result.getFailed()));
    assertFasterThanLimit(update);
    assertTrue(isAttachedPartition(partition3()), "The partition _p3 must have been created and attached");

    TimedResponse nextUpdate = invoke(new PSQLXyzConnector(), updateEvent(false, FEATURE_IDS));
    FeatureCollection nextResult = deserializeResponse(nextUpdate.response());
    assertEquals(Set.copyOf(FEATURE_IDS), Set.copyOf(nextResult.getUpdated()), "Unexpected result: " + nextUpdate.response());
    assertEquals(List.of(), failedIds(nextResult.getFailed()));
    assertFasterThanLimit(nextUpdate);
  }

  @Test
  void updateFailsAsBeforeWhenRecoveryCannotCreatePartition() throws Exception {
    //A plain table with the partition's name makes "CREATE TABLE IF NOT EXISTS ... PARTITION OF" a no-op
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      new SQLQuery("CREATE TABLE ${schema}.${table} (x INT)")
          .withVariable("schema", PG_SCHEMA)
          .withVariable("table", partition3())
          .write(dsp);
    }

    TimedResponse transactional = invoke(new PSQLXyzConnector(), updateEvent(true, FEATURE_IDS));
    XyzResponse transactionalResult = XyzSerializable.deserialize(transactional.response());
    assertTrue(transactionalResult instanceof ErrorResponse, "Expected an error response: " + transactional.response());
    ErrorResponse error = (ErrorResponse) transactionalResult;
    assertEquals(XyzError.CONFLICT, error.getError());
    assertEquals(DatabaseWriter.TRANSACTION_ERROR_GENERAL, error.getErrorMessage());
    List<Map<String, Object>> failedList = (List<Map<String, Object>>) error.getErrorDetails().get("FailedList");
    assertEquals(FEATURE_IDS, failedList.stream().map(failure -> (String) failure.get("id")).sorted().toList());
    assertFalse(transactional.response().contains("Batch entry"), "The response must not expose the batch statement");
    assertFasterThanLimit(transactional);

    TimedResponse nonTransactional = invoke(new PSQLXyzConnector(), updateEvent(false, FEATURE_IDS));
    FeatureCollection nonTransactionalResult = deserializeResponse(nonTransactional.response());
    assertEquals(FEATURE_IDS, failedIds(nonTransactionalResult.getFailed()).stream().sorted().toList());
    assertNull(nonTransactionalResult.getUpdated(), "No feature may be reported as updated: " + nonTransactional.response());
    assertFasterThanLimit(nonTransactional);
  }

  private record TimedResponse(String response, long millis) {}

  private static TimedResponse invoke(PSQLXyzConnector connector, ModifyFeaturesEvent event) throws Exception {
    long start = System.nanoTime();
    String response = invokeLambda(connector, event);
    long millis = (System.nanoTime() - start) / 1_000_000;
    LOGGER.info("ModifyFeaturesEvent (transaction: {}) took {} ms", event.getTransaction(), millis);
    return new TimedResponse(response, millis);
  }

  private static void assertFasterThanLimit(TimedResponse timedResponse) {
    assertTrue(timedResponse.millis() < MAX_REQUEST_MILLIS,
        "The request took " + timedResponse.millis() + " ms, the limit is " + MAX_REQUEST_MILLIS + " ms");
  }

  private ModifyFeaturesEvent updateEvent(boolean transactional, List<String> ids) {
    return new ModifyFeaturesEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace(spaceId)
        .withVersionsToKeep(1000)
        .withTransaction(transactional)
        .withUpdateFeatures(features(ids, "updated-" + Random.randomAlpha(4)));
  }

  private static List<Feature> features(List<String> ids, String value) {
    //A mutable list, because the connector adds to the lists of the event
    return new ArrayList<>(ids.stream()
        .map(id -> new Feature().withId(id).withProperties(new Properties().withXyzNamespace(new XyzNamespace()).with("value", value)))
        .toList());
  }

  private static List<String> failedIds(List<ModificationFailure> failures) {
    return failures == null ? List.of() : failures.stream().map(ModificationFailure::getId).toList();
  }

  private String partition3() {
    return spaceId + "_p3";
  }

  private static <R> R query(SQLQuery query, ResultSetHandler<R> handler) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      return query.run(dsp, handler);
    }
  }

  private static boolean tableExists(String table) throws Exception {
    return query(new SQLQuery("SELECT to_regclass(#{table}) IS NOT NULL")
        .withNamedParameter("table", PG_SCHEMA + ".\"" + table + "\""), rs -> rs.next() && rs.getBoolean(1));
  }

  private boolean isAttachedPartition(String table) throws Exception {
    return query(new SQLQuery("SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(#{child}) AND inhparent = to_regclass(#{parent})")
        .withNamedParameter("child", PG_SCHEMA + ".\"" + table + "\"")
        .withNamedParameter("parent", PG_SCHEMA + ".\"" + spaceId + "\""), rs -> rs.next());
  }

  private static long countRows(String table) throws Exception {
    return query(new SQLQuery("SELECT count(*) FROM ${schema}.${table}")
        .withVariable("schema", PG_SCHEMA)
        .withVariable("table", table), rs -> rs.next() ? rs.getLong(1) : -1);
  }
}
