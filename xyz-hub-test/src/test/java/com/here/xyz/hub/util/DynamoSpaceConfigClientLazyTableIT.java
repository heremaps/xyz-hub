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

package com.here.xyz.hub.util;

import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.ABSENT;
import static com.here.xyz.hub.config.SpaceConfigClient.ClearResult.REMOVED;
import static com.here.xyz.models.hub.Space.LAZY_TABLE;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient.ClearResult;
import com.here.xyz.hub.config.dynamo.DynamoSpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.task.LazyTableClearing;
import com.here.xyz.models.hub.Space.ConnectorRef;
import io.vertx.core.Future;
import java.net.URI;
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

/**
 * The conditional clear of the lazyTable flag on DynamoDB local. Its result decides whether the Hub broadcasts a global cache
 * invalidation, so REMOVED and ABSENT must come from the old attributes DynamoDB returns.
 */
class DynamoSpaceConfigClientLazyTableIT extends DynamoDbIT {

  private static final Marker MARKER = MarkerManager.getMarker(DynamoSpaceConfigClientLazyTableIT.class.getSimpleName());
  private static final String TABLE_PREFIX = DynamoSpaceConfigClientLazyTableIT.class.getSimpleName() + "_"
      + RandomStringUtils.randomAlphanumeric(5);
  private static final String SPACES_TABLE = TABLE_PREFIX + "-spaces";
  private static final String PACKAGES_TABLE = TABLE_PREFIX + "-packages";
  private static final String CONNECTOR = "psql";

  private Config previousConfiguration;
  private CountingClient spaces;

  @BeforeEach
  void setUp() {
    previousConfiguration = Service.configuration;
    Service.configuration = new Config();
    Service.configuration.PACKAGES_DYNAMODB_TABLE_ARN = tableArn(PACKAGES_TABLE);
    spaces = new CountingClient(tableArn(SPACES_TABLE));
    awaitResult(spaces.init());

    awaitTableActive();
  }

  @AfterEach
  void restoreConfigurationAndDropPackages() {
    Service.configuration = previousConfiguration;
    try {
      rawDynamoClient.deleteTable(PACKAGES_TABLE);
    }
    catch (ResourceNotFoundException ignored) {
      //Table may already be deleted
    }
  }

  @Test
  void clearRemovesTheFlagOnceAndKeepsTheOtherAttributes() {
    Space space = lazySpace();
    awaitResult(spaces.store(MARKER, space));
    spaces.invalidations.clear();
    Map<String, Object> before = item(space.getId());
    assertThat(params(before)).containsEntry(LAZY_TABLE, true);
    Map<String, Object> expected = item(space.getId());
    params(expected).remove(LAZY_TABLE);

    assertThat(clear(space)).isEqualTo(REMOVED);
    assertThat(spaces.invalidations).containsExactly(space.getId());
    assertThat(item(space.getId())).isEqualTo(expected);

    assertThat(clear(space)).isEqualTo(ABSENT);
    assertThat(spaces.invalidations).containsExactly(space.getId());
    assertThat(item(space.getId())).isEqualTo(expected);
  }

  @Override
  protected String tableName() {
    return SPACES_TABLE;
  }

  private ClearResult clear(Space space) {
    String tableName = (String) space.getStorage().getParams().get(TABLE_NAME);
    return awaitResult(spaces.clearLazyTable(MARKER, space.getId(), CONNECTOR, tableName, LazyTableClearing.SOURCE_ADMIN_API));
  }

  private static Space lazySpace() {
    String spaceId = TABLE_PREFIX + "-space";
    Map<String, Object> params = new HashMap<>();
    params.put(TABLE_NAME, spaceId + "_table");
    params.put(LAZY_TABLE, true);
    params.put("other", "kept");
    Space space = new Space();
    space.setId(spaceId);
    space.setOwner("lazy-table-owner");
    space.setTitle("title of " + spaceId);
    space.setStorage(new ConnectorRef().withId(CONNECTOR).withParams(params));
    return space;
  }

  /**
   * @return The stored item of the space, as plain values
   */
  private static Map<String, Object> item(String spaceId) {
    Map<String, AttributeValue> item = rawDynamoClient.getItem(SPACES_TABLE, Map.of("id", new AttributeValue(spaceId))).getItem();
    assertThat(item).isNotNull();
    return ItemUtils.toSimpleMapValue(item);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> params(Map<String, Object> item) {
    return (Map<String, Object>) ((Map<String, Object>) item.get("storage")).get("params");
  }

  /**
   * @return The ARN of a table of DynamoDB local, from which DynamoClient derives the endpoint
   */
  private static String tableArn(String tableName) {
    URI uri = URI.create(endpoint);
    return "arn:aws:dynamodb:" + uri.getHost() + ":" + String.format("%012d", uri.getPort()) + ":table/" + tableName;
  }

  private static <T> T awaitResult(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().join();
  }

  /**
   * Counts cache invalidations instead of broadcasting them.
   */
  private static class CountingClient extends DynamoSpaceConfigClient {

    final List<String> invalidations = new ArrayList<>();

    CountingClient(String tableArn) {
      super(tableArn);
    }

    @Override
    public void invalidateCache(String spaceId) {
      invalidations.add(spaceId);
      cache.remove(spaceId);
    }
  }
}
