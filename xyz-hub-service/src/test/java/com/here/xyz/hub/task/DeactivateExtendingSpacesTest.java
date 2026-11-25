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

package com.here.xyz.hub.task;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.config.dynamo.DynamoSpaceConfigClient;
import com.here.xyz.hub.config.memory.InMemSpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Space.Extension;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DS-2872: deleting a base deactivates its extensions. An extension deleted in parallel must not be stored again.
 */
class DeactivateExtendingSpacesTest {

  private static final Marker MARKER = MarkerManager.getMarker(DeactivateExtendingSpacesTest.class.getSimpleName());

  private RecordingClient client;
  private SpaceConfigClient previousSpaceConfigClient;

  @BeforeEach
  void setUp() {
    client = new RecordingClient();
    previousSpaceConfigClient = Service.spaceConfigClient;
    Service.spaceConfigClient = client;
  }

  @AfterEach
  void tearDown() {
    Service.spaceConfigClient = previousSpaceConfigClient;
  }

  @Test
  void deactivatesDirectAndNestedExtensionsAndKeepsTheirOtherAttributes() {
    Space base = stored(space(null));
    Space extension = stored(space(base.getId()));
    Space nested = stored(space(extension.getId()));

    await(SpaceTaskHandler.deactivateExtendingSpaces(MARKER, base.getId()));

    assertThat(storedSpace(extension.getId()).isActive()).isFalse();
    assertThat(storedSpace(nested.getId()).isActive()).isFalse();
    assertThat(storedSpace(extension.getId()).getTitle()).isEqualTo(extension.getTitle());
    assertThat(storedSpace(base.getId()).isActive()).isTrue();
    assertThat(client.invalidations).containsExactlyInAnyOrder(extension.getId(), nested.getId());
  }

  @Test
  void anExtensionDeletedAfterTheLookupIsNotStoredAgain() {
    Space base = stored(space(null));
    Space extension = stored(space(base.getId()));
    //The catalog deletion deletes the extension right after the base deletion listed it
    client.deleteAfterLookup.add(extension.getId());

    await(SpaceTaskHandler.deactivateExtendingSpaces(MARKER, base.getId()));

    assertThat(storedSpace(extension.getId())).isNull();
    assertThat(client.storedSpaces).isEmpty();
    assertThat(client.invalidations).isEmpty();
  }

  @Test
  void deactivatingAMissingSpaceCreatesNothing() {
    String missingId = uniqueId();

    assertThat(await(client.deactivate(MARKER, missingId))).isFalse();

    assertThat(storedSpace(missingId)).isNull();
    assertThat(client.storedSpaces).isEmpty();
  }

  @Test
  void dynamoDbRequestOnlySetsActiveAndRequiresAnExistingItem() {
    UpdateItemRequest request = DynamoSpaceConfigClient.buildDeactivateRequest("spaces", "s1");

    assertThat(request.getTableName()).isEqualTo("spaces");
    assertThat(request.getKey()).isEqualTo(Map.of("id", new AttributeValue("s1")));
    assertThat(request.getUpdateExpression()).isEqualTo("SET #a = :false");
    assertThat(request.getConditionExpression()).isEqualTo("attribute_exists(#i)");
    assertThat(request.getExpressionAttributeNames()).isEqualTo(Map.of("#a", "active", "#i", "id"));
    assertThat(request.getExpressionAttributeValues()).isEqualTo(Map.of(":false", new AttributeValue().withBOOL(false)));
  }

  private Space stored(Space space) {
    await(client.storeSpace(MARKER, space));
    client.storedSpaces.clear();
    return space;
  }

  private Space storedSpace(String spaceId) {
    return await(client.getSpace(MARKER, spaceId));
  }

  private static Space space(String baseId) {
    Space space = new Space();
    space.setId(uniqueId());
    space.setTitle("title of " + space.getId());
    if (baseId != null)
      space.setExtension(new Extension().withSpaceId(baseId));
    return space;
  }

  private static String uniqueId() {
    return DeactivateExtendingSpacesTest.class.getSimpleName() + "_" + RandomStringUtils.randomAlphanumeric(5);
  }

  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().join();
  }

  /**
   * Records stores and cache invalidations instead of broadcasting them. Deletes the listed spaces after they were returned as
   * extensions, like a deletion running in parallel.
   */
  private static class RecordingClient extends InMemSpaceConfigClient {
    final List<String> invalidations = new ArrayList<>();
    final List<Space> storedSpaces = new ArrayList<>();
    final Set<String> deleteAfterLookup = new HashSet<>();

    @Override
    public Future<List<Space>> getSpacesFromSuper(Marker marker, String superSpaceId) {
      return super.getSpacesFromSuper(marker, superSpaceId)
          .onSuccess(spaces -> spaces.forEach(space -> {
            if (deleteAfterLookup.remove(space.getId()))
              deleteSpace(marker, space.getId());
          }));
    }

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
