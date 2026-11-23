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

package com.here.xyz.hub.config.memory;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.ABSENT;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.CONDITION_FAILED;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.REMOVED;
import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.config.SpaceConfigClient.ClearResult;
import com.here.xyz.hub.config.dynamo.DynamoSpaceConfigClient;
import com.here.xyz.hub.config.jdbc.JDBCSpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.task.LazyTableClearing;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemSpaceConfigClientLazyTableTest {

  private static final Marker MARKER = MarkerManager.getMarker("InMemSpaceConfigClientLazyTableTest");

  private CountingClient client;
  private SpaceConfigClient previousSpaceConfigClient;

  @BeforeEach
  void setUp() {
    client = new CountingClient();
    previousSpaceConfigClient = Service.spaceConfigClient;
    Service.spaceConfigClient = client;
  }

  @AfterEach
  void tearDown() {
    Service.spaceConfigClient = previousSpaceConfigClient;
  }

  @Test
  void uniqueNameRemovesTheFlagAndInvalidatesGlobally() {
    Space space = stored(space("psql", "t1", true));

    assertThat(clear(space.getId(), "psql", "t1")).isEqualTo(REMOVED);
    Space cleared = storedSpace(space.getId());
    assertThat(cleared.getStorage().getParams()).doesNotContainKey(LAZY_TABLE).containsEntry(TABLE_NAME, "t1")
        .containsEntry("other", "kept");
    assertThat(cleared.getTitle()).isEqualTo(space.getTitle());
    assertThat(client.invalidations).containsExactly(space.getId());
  }

  @Test
  void aRepeatIsAbsentAndOnlyRemovesTheLocalCacheEntry() {
    Space space = stored(space("psql", "t1", true));
    clear(space.getId(), "psql", "t1");
    SpaceConfigClient.cache.put(space.getId(), space);

    assertThat(clear(space.getId(), "psql", "t1")).isEqualTo(ABSENT);
    assertThat(client.invalidations).containsExactly(space.getId());
    assertThat(SpaceConfigClient.cache.get(space.getId())).isNull();
  }

  @Test
  void legacyFormRemovesTheFlagOfASpaceWithoutTableName() {
    Space space = stored(space("psql", null, true));

    assertThat(clear(space.getId(), "psql", null)).isEqualTo(REMOVED);
    assertThat(storedSpace(space.getId()).getStorage().getParams()).doesNotContainKey(LAZY_TABLE).containsEntry("other", "kept");
  }

  @Test
  void conditionFailsForAnotherConnectorOrTableNameAndKeepsTheFlag() {
    Space unique = stored(space("psql", "t1", true));
    Space legacy = stored(space("psql", null, true));
    SpaceConfigClient.cache.put(unique.getId(), unique);

    assertThat(clear(unique.getId(), "psql-db2", "t1")).isEqualTo(CONDITION_FAILED);
    assertThat(clear(unique.getId(), "psql", "t2")).isEqualTo(CONDITION_FAILED);
    assertThat(clear(unique.getId(), "psql", null)).isEqualTo(CONDITION_FAILED);
    assertThat(clear(legacy.getId(), "psql", "t1")).isEqualTo(CONDITION_FAILED);
    assertThat(clear(legacy.getId(), "psql-db2", null)).isEqualTo(CONDITION_FAILED);

    assertThat(storedSpace(unique.getId()).getStorage().getParams()).containsEntry(LAZY_TABLE, true);
    assertThat(storedSpace(legacy.getId()).getStorage().getParams()).containsEntry(LAZY_TABLE, true);
    assertThat(client.invalidations).isEmpty();
    assertThat(SpaceConfigClient.cache.get(unique.getId())).isSameAs(unique);
    SpaceConfigClient.cache.remove(unique.getId());
  }

  @Test
  void conditionFailsForAnUnknownSpace() {
    String unknownId = uniqueId();

    assertThat(clear(unknownId, "psql", "t1")).isEqualTo(CONDITION_FAILED);
    assertThat(await(client.getSpace(MARKER, unknownId))).isNull();
  }

  @Test
  void conditionFailsForALegacySpaceWithoutStorageParams() {
    Space space = space("psql", null, false);
    space.getStorage().setParams(null);
    stored(space);

    assertThat(clear(space.getId(), "psql", null)).isEqualTo(CONDITION_FAILED);
    assertThat(storedSpace(space.getId()).getStorage().getParams()).isNull();
  }

  @Test
  void theStoredObjectIsReplacedNeverMutated() {
    Space space = stored(space("psql", "t1", true));
    Space heldBefore = storedSpace(space.getId());

    clear(space.getId(), "psql", "t1");

    assertThat(heldBefore).isSameAs(space);
    assertThat(heldBefore.getStorage().getParams()).containsEntry(LAZY_TABLE, true);
    assertThat(storedSpace(space.getId())).isNotSameAs(heldBefore);
  }

  @Test
  void aStaleCacheInstanceGetsAbsentAndItsDeferredStoreKeepsTheFlagCleared() {
    Space space = stored(space("psql", "t1", true));
    //Two requests hold the same cached instance, which carries the flag
    Space stale = storedSpace(space.getId());
    assertThat(clear(space.getId(), "psql", "t1")).isEqualTo(REMOVED);
    client.invalidations.clear();

    ClearResult result = await(LazyTableClearing.clearAfterWrite(MARKER, stale, DEFAULT, true));

    assertThat(result).isEqualTo(ABSENT);
    assertThat(client.invalidations).isEmpty();

    stale.updateContentUpdatedAt(MARKER, LazyTableClearing.isCleared(result));

    Space storedAfter = storedSpace(space.getId());
    assertThat(storedAfter).isNotSameAs(stale);
    assertThat(storedAfter.getStorage().getParams()).doesNotContainKey(LAZY_TABLE).containsEntry(TABLE_NAME, "t1");
    assertThat(storedAfter.getContentUpdatedAt()).isEqualTo(stale.getContentUpdatedAt()).isPositive();
    assertThat(stale.getStorage().getParams()).containsEntry(LAZY_TABLE, true);
  }

  @Test
  void aConcurrentWholeItemWriterPutsTheFlagBackAndTheNextReportRemovesItAgain() {
    Space space = stored(space("psql", "t1", true));
    Space loadedBeforeTheClear = storedSpace(space.getId());

    assertThat(clear(space.getId(), "psql", "t1")).isEqualTo(REMOVED);
    await(client.store(MARKER, loadedBeforeTheClear));
    assertThat(storedSpace(space.getId()).getStorage().getParams()).containsEntry(LAZY_TABLE, true);

    assertThat(clear(space.getId(), "psql", "t1")).isEqualTo(REMOVED);
    assertThat(storedSpace(space.getId()).getStorage().getParams()).doesNotContainKey(LAZY_TABLE);
  }

  @Test
  void deferredStoreWritesACopyWithoutTheFlag() {
    Space space = stored(space("psql", "t1", true));
    client.storedSpaces.clear();

    space.updateContentUpdatedAt(MARKER, true);

    assertThat(client.storedSpaces).hasSize(1);
    Space storedCopy = client.storedSpaces.get(0);
    assertThat(storedCopy).isNotSameAs(space);
    assertThat(storedCopy.getStorage().getParams()).doesNotContainKey(LAZY_TABLE).containsEntry(TABLE_NAME, "t1")
        .containsEntry("other", "kept");
    assertThat(storedCopy.getId()).isEqualTo(space.getId());
    assertThat(storedCopy.getTitle()).isEqualTo(space.getTitle());
    assertThat(storedCopy.getContentUpdatedAt()).isEqualTo(space.getContentUpdatedAt()).isPositive();
    //The shared object keeps its flag, only its timestamp changed
    assertThat(space.getStorage().getParams()).containsEntry(LAZY_TABLE, true);
  }

  @Test
  void storeWithTheFlagStoresTheObjectItself() {
    Space space = stored(space("psql", "t1", true));
    client.storedSpaces.clear();

    space.updateContentUpdatedAt(MARKER, false);

    assertThat(client.storedSpaces).containsExactly(space);
    assertThat(storedSpace(space.getId()).getStorage().getParams()).containsEntry(LAZY_TABLE, true);
  }

  @Test
  void theThrottleAppliesToBothForms() {
    Space space = stored(space("psql", "t1", true));
    client.storedSpaces.clear();

    space.updateContentUpdatedAt(MARKER, true);
    space.updateContentUpdatedAt(MARKER, true);
    space.updateContentUpdatedAt(MARKER, false);
    space.updateContentUpdatedAt(MARKER);

    assertThat(client.storedSpaces).hasSize(1);
  }

  @Test
  void dynamoDbRequestOfTheUniqueForm() {
    UpdateItemRequest request = DynamoSpaceConfigClient.buildClearLazyTableRequest("spaces", "s1", "psql", "t1");

    assertThat(DynamoSpaceConfigClient.CLEAR_LAZY_TABLE_UPDATE_EXPRESSION).isEqualTo("REMOVE #s.#p.#l");
    assertThat(DynamoSpaceConfigClient.CLEAR_LAZY_TABLE_CONDITION).isEqualTo("#s.#i = :connector AND #s.#p.#t = :table");
    assertThat(request.getTableName()).isEqualTo("spaces");
    assertThat(request.getKey()).isEqualTo(Map.of("id", new AttributeValue("s1")));
    assertThat(request.getUpdateExpression()).isEqualTo("REMOVE #s.#p.#l");
    assertThat(request.getConditionExpression()).isEqualTo("#s.#i = :connector AND #s.#p.#t = :table");
    assertThat(request.getExpressionAttributeNames())
        .isEqualTo(Map.of("#s", "storage", "#p", "params", "#l", "lazyTable", "#i", "id", "#t", "tableName"));
    assertThat(request.getExpressionAttributeValues())
        .isEqualTo(Map.of(":connector", new AttributeValue().withS("psql"), ":table", new AttributeValue().withS("t1")));
    assertThat(request.getReturnValues()).isEqualTo(ReturnValue.UPDATED_OLD.toString());
  }

  @Test
  void dynamoDbRequestOfTheLegacyForm() {
    UpdateItemRequest request = DynamoSpaceConfigClient.buildClearLazyTableRequest("spaces", "s1", "psql", null);

    assertThat(DynamoSpaceConfigClient.CLEAR_LAZY_TABLE_LEGACY_CONDITION)
        .isEqualTo("#s.#i = :connector AND attribute_exists(#s.#p) AND attribute_not_exists(#s.#p.#t)");
    assertThat(request.getConditionExpression())
        .isEqualTo("#s.#i = :connector AND attribute_exists(#s.#p) AND attribute_not_exists(#s.#p.#t)");
    assertThat(request.getExpressionAttributeValues()).isEqualTo(Map.of(":connector", new AttributeValue().withS("psql")));
    assertThat(request.getExpressionAttributeNames()).containsOnlyKeys("#s", "#p", "#l", "#i", "#t");
  }

  @Test
  void dynamoDbOldAttributesTellRemovedFromAbsent() {
    AttributeValue params = new AttributeValue().withM(Map.of(LAZY_TABLE, new AttributeValue().withBOOL(true)));
    Map<String, AttributeValue> withFlag = Map.of("storage", new AttributeValue().withM(Map.of("params", params)));

    assertThat(DynamoSpaceConfigClient.clearResultOf(withFlag)).isEqualTo(REMOVED);
    assertThat(DynamoSpaceConfigClient.clearResultOf(null)).isEqualTo(ABSENT);
    assertThat(DynamoSpaceConfigClient.clearResultOf(Map.of())).isEqualTo(ABSENT);
  }

  @Test
  void jdbcStatementsOfTheUniqueForm() {
    SQLQuery update = JDBCSpaceConfigClient.buildClearLazyTableQuery("s1", "psql", "t1");
    SQLQuery check = JDBCSpaceConfigClient.buildClearLazyTableCheckQuery("s1", "psql", "t1");

    //One changed row: REMOVED
    assertThat(update.text()).isEqualTo("UPDATE ${schema}.${table} SET config = config #- '{storage,params,lazyTable}' "
        + "WHERE id = #{spaceId} AND config #>> '{storage,id}' = #{connector} AND config #>> '{storage,params,tableName}' = #{table} "
        + "AND config #> '{storage,params,lazyTable}' IS NOT NULL");
    //No changed row, then a row: ABSENT; no row: CONDITION_FAILED
    assertThat(check.text()).isEqualTo("SELECT 1 FROM ${schema}.${table} "
        + "WHERE id = #{spaceId} AND config #>> '{storage,id}' = #{connector} AND config #>> '{storage,params,tableName}' = #{table}");
    Map<String, Object> parameters = Map.of("spaceId", "s1", "connector", "psql", "table", "t1");
    assertThat(update.getNamedParameters()).isEqualTo(parameters);
    assertThat(check.getNamedParameters()).isEqualTo(parameters);
  }

  @Test
  void jdbcStatementsOfTheLegacyForm() {
    SQLQuery update = JDBCSpaceConfigClient.buildClearLazyTableQuery("s1", "psql", null);
    SQLQuery check = JDBCSpaceConfigClient.buildClearLazyTableCheckQuery("s1", "psql", null);

    String legacyCondition = "WHERE id = #{spaceId} AND config #>> '{storage,id}' = #{connector} "
        + "AND config #> '{storage,params}' IS NOT NULL AND config #> '{storage,params,tableName}' IS NULL";
    assertThat(update.text()).isEqualTo("UPDATE ${schema}.${table} SET config = config #- '{storage,params,lazyTable}' "
        + legacyCondition + " AND config #> '{storage,params,lazyTable}' IS NOT NULL");
    assertThat(check.text()).isEqualTo("SELECT 1 FROM ${schema}.${table} " + legacyCondition);
    Map<String, Object> parameters = Map.of("spaceId", "s1", "connector", "psql");
    assertThat(update.getNamedParameters()).isEqualTo(parameters);
    assertThat(check.getNamedParameters()).isEqualTo(parameters);
  }

  private ClearResult clear(String spaceId, String connectorId, String tableName) {
    return await(client.clearLazyTable(MARKER, spaceId, connectorId, tableName, LazyTableClearing.SOURCE_ADMIN_API));
  }

  private Space stored(Space space) {
    await(client.storeSpace(MARKER, space));
    client.invalidations.clear();
    client.storedSpaces.clear();
    return space;
  }

  private Space storedSpace(String spaceId) {
    return await(client.getSpace(MARKER, spaceId));
  }

  private static Space space(String storageId, String tableName, boolean lazy) {
    Map<String, Object> params = new HashMap<>();
    params.put("other", "kept");
    if (tableName != null)
      params.put(TABLE_NAME, tableName);
    if (lazy)
      params.put(LAZY_TABLE, true);
    Space space = new Space();
    space.setId(uniqueId());
    space.setTitle("title of " + space.getId());
    space.setStorage(new ConnectorRef().withId(storageId).withParams(params));
    return space;
  }

  private static String uniqueId() {
    return InMemSpaceConfigClientLazyTableTest.class.getSimpleName() + "_" + RandomStringUtils.randomAlphanumeric(5);
  }

  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().join();
  }

  /**
   * Counts cache invalidations instead of broadcasting them, and records the stored spaces.
   */
  private static class CountingClient extends InMemSpaceConfigClient {

    final List<String> invalidations = new ArrayList<>();
    final List<Space> storedSpaces = new ArrayList<>();

    @Override
    public Future<Void> storeSpace(Marker marker, Space space) {
      storedSpaces.add(space);
      return super.storeSpace(marker, space);
    }

    @Override
    public void invalidateCache(String spaceId) {
      invalidations.add(spaceId);
      cache.remove(spaceId);
    }
  }
}
