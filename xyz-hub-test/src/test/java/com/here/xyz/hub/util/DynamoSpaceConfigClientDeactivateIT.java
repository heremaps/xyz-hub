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

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.here.xyz.hub.Config;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoSpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import com.here.xyz.models.hub.Space.Extension;
import io.vertx.core.Future;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DS-2872: the deactivation of an extension on DynamoDB local. It changes only {@code active} and never re-creates a space that
 * was deleted in parallel to the deletion of its base.
 */
class DynamoSpaceConfigClientDeactivateIT extends DynamoDbIT {

  private static final Marker MARKER = MarkerManager.getMarker(DynamoSpaceConfigClientDeactivateIT.class.getSimpleName());
  private static final String TABLE_PREFIX = DynamoSpaceConfigClientDeactivateIT.class.getSimpleName() + "_"
      + RandomStringUtils.randomAlphanumeric(5);
  private static final String SPACES_TABLE = TABLE_PREFIX + "-spaces";
  private static final String PACKAGES_TABLE = TABLE_PREFIX + "-packages";

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

  @Override
  protected String tableName() {
    return SPACES_TABLE;
  }

  @Test
  void deactivateSetsOnlyActive() {
    Space extension = extension("ext");
    awaitResult(spaces.store(MARKER, extension));
    spaces.invalidations.clear();
    Map<String, Object> expected = item(extension.getId());
    assertThat(expected).doesNotContainKey("active");
    expected.put("active", false);

    assertThat(awaitResult(spaces.deactivate(MARKER, extension.getId()))).isTrue();

    assertThat(item(extension.getId())).isEqualTo(expected);
    assertThat(spaces.invalidations).containsExactly(extension.getId());
    Space loaded = awaitResult(spaces.get(MARKER, extension.getId()));
    assertThat(loaded.isActive()).isFalse();
    assertThat(loaded.getExtension().getSpaceId()).isEqualTo(extension.getExtension().getSpaceId());
  }

  @Test
  void deactivateDoesNotRecreateADeletedSpace() {
    Space extension = extension("ext-deleted");
    awaitResult(spaces.store(MARKER, extension));
    //The lookup of the extending spaces happens before the parallel deletion and caches the space
    assertThat(awaitResult(spaces.getSpacesFromSuper(MARKER, extension.getExtension().getSpaceId())))
        .extracting(Space::getId).containsExactly(extension.getId());
    awaitResult(spaces.delete(MARKER, extension.getId()));
    spaces.invalidations.clear();

    assertThat(awaitResult(spaces.deactivate(MARKER, extension.getId()))).isFalse();

    assertThat(rawItem(extension.getId())).isNull();
    assertThat(spaces.invalidations).isEmpty();
  }

  @Test
  void deactivateOfAnUnknownSpaceCreatesNothing() {
    String unknownId = TABLE_PREFIX + "-unknown";

    assertThat(awaitResult(spaces.deactivate(MARKER, unknownId))).isFalse();

    assertThat(rawItem(unknownId)).isNull();
  }

  private static Space extension(String suffix) {
    String spaceId = TABLE_PREFIX + "-" + suffix;
    Space space = new Space();
    space.setId(spaceId);
    space.setOwner("deactivate-owner");
    space.setTitle("title of " + spaceId);
    space.setStorage(new ConnectorRef().withId("psql").withParams(Map.of("other", "kept")));
    space.setExtension(new Extension().withSpaceId(TABLE_PREFIX + "-base"));
    return space;
  }

  private static Map<String, AttributeValue> rawItem(String spaceId) {
    return rawDynamoClient.getItem(SPACES_TABLE, Map.of("id", new AttributeValue(spaceId))).getItem();
  }

  /**
   * @return The stored item of the space, as plain values
   */
  private static Map<String, Object> item(String spaceId) {
    Map<String, AttributeValue> item = rawItem(spaceId);
    assertThat(item).isNotNull();
    return ItemUtils.toSimpleMapValue(item);
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
