/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

package com.here.xyz.hub.config;

import com.google.common.util.concurrent.Monitor;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoSpaceConfigClient;
import com.here.xyz.hub.config.jdbc.JDBCSpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class SpaceConfigClient implements Initializable {

  private static final Logger logger = LogManager.getLogger();

  public static final ExpiringMap<String, Space> cache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(3, TimeUnit.MINUTES)
      .build();

  private static final Map<String, ConcurrentLinkedQueue<Promise<Space>>> pendingGetCalls = new ConcurrentHashMap<>();
  private static final Map<String, Monitor> getSpaceLocks = new ConcurrentHashMap<>();
  private SpaceSelectionCondition emptySpaceCondition = new SpaceSelectionCondition();

  //Property keys for PropertyQuery
  public static final String CONTENT_UPDATED_AT = "contentUpdatedAt";
  public static final String UPDATED_AT = "updatedAt";

  public static SpaceConfigClient getInstance() {
    if (Service.configuration.SPACES_DYNAMODB_TABLE_ARN != null) {
      return new DynamoSpaceConfigClient(Service.configuration.SPACES_DYNAMODB_TABLE_ARN);
    } else {
      return JDBCSpaceConfigClient.getInstance();
    }
  }

  public Future<Space> get(Marker marker, String spaceId) {
    Space cached = cache.get(spaceId);
    if (cached != null) {
      logger.info(marker, "space[{}]: Loaded space with title \"{}\" from cache", spaceId, cached.getTitle());
      return Future.succeededFuture(cached);
    }

    /*
    In case we get the query for a space of which a previous request is already in flight we wait for its response and call the callback
    then. This is a performance optimization for highly parallel requests coming from the user at once.
     */
    getSpaceLocks.putIfAbsent(spaceId, new Monitor());
    Promise<Space> p = Promise.promise();
    try {
      getSpaceLocks.get(spaceId).enter();
      boolean isFirstRequest = pendingGetCalls.putIfAbsent(spaceId, new ConcurrentLinkedQueue<>()) == null;
      pendingGetCalls.get(spaceId).add(p);
      if (!isFirstRequest) {
        return p.future();
      }
    }
    finally {
      getSpaceLocks.get(spaceId).leave();
    }

    getSpace(marker, spaceId).onComplete(ar -> {
      ConcurrentLinkedQueue<Promise<Space>> handlersToCall;
      try {
        getSpaceLocks.get(spaceId).enter();
        handlersToCall = pendingGetCalls.remove(spaceId);
      }
      finally {
        getSpaceLocks.get(spaceId).leave();
      }
      if (ar.succeeded()) {
        Space space = ar.result();
        if (space != null) {
          cache.put(spaceId, space);
          logger.info(marker, "space[{}]: Loaded space with title: \"{}\"", spaceId, space.getTitle());
        } else {
          logger.info(marker, "space[{}]: Space with this ID was not found", spaceId);
        }
        cache.put(spaceId, space);
        handlersToCall.forEach(h -> h.complete(ar.result()));
      }
      else {
        logger.error(marker, "space[{}]: Failed to load the space, reason: {}", spaceId, ar.cause());
        handlersToCall.forEach(h -> h.fail(ar.cause()));
      }
    });

    return p.future();
  }

  public Future<Void> store(Marker marker, Space space) {
    if (space.getId() == null)
      space.setId(RandomStringUtils.randomAlphanumeric(10));
    return storeSpace(marker, space)
        .onSuccess(v -> {
          invalidateCache(space.getId());
          logger.info(marker, "space[{}]: Stored successfully with title: \"{}\"", space.getId(), space.getTitle());
        })
        .onFailure(t -> logger.error(marker, "space[{}]: Failed storing the space", space.getId(), t));
  }

  public Future<Space> delete(Marker marker, String spaceId) {
    return deleteSpace(marker, spaceId)
        .onSuccess(space -> {
          invalidateCache(spaceId);
          logger.info(marker, "space[{}]: Deleted space", spaceId);
        })
        .onFailure(t -> logger.error(marker, "space[{}]: Failed deleting the space", spaceId, t));
  }

  public Future<List<Space>> getSelected(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery) {
    return getSelectedSpaces(marker, authorizedCondition, selectedCondition, propsQuery)
        .onSuccess(spaces -> {
          spaces.forEach(s -> cache.put(s.getId(), s));
          logger.info(marker, "Loaded spaces by condition");
        })
        .onFailure(t -> logger.error(marker, "Failed to load spaces by condition", t));
  }

  public Future<List<Space>> getSpacesForOwner(Marker marker, String ownerId) {
    SpaceSelectionCondition selectedCondition = new SpaceSelectionCondition();
    selectedCondition.ownerIds = Collections.singleton(ownerId);
    selectedCondition.shared = false;
    return getSelected(marker, emptySpaceCondition, selectedCondition, null);
  }

  protected abstract Future<Space> getSpace(Marker marker, String spaceId);

  protected abstract Future<Void> storeSpace(Marker marker, Space space);

  protected abstract Future<Space> deleteSpace(Marker marker, String spaceId);

  protected abstract Future<List<Space>> getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, PropertiesQuery propsQuery);

  public void invalidateCache(String spaceId) {
    cache.remove(spaceId);
    new InvalidateSpaceCacheMessage().withId(spaceId).withGlobalRelay(true).broadcast();
  }

  public static class SpaceAuthorizationCondition {
    public Set<String> spaceIds;
    public Set<String> ownerIds;
    public Set<String> packages;
  }

  public static class SpaceSelectionCondition extends SpaceAuthorizationCondition {

    public boolean shared = true;
    public boolean negateOwnerIds = false;
    public String tagId;
    public String region;
  }

  public static class InvalidateSpaceCacheMessage extends RelayedMessage {

    private String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public InvalidateSpaceCacheMessage withId(String id) {
      this.id = id;
      return this;
    }

    @Override
    protected void handleAtDestination() {
      cache.remove(id);
    }
  }
}
