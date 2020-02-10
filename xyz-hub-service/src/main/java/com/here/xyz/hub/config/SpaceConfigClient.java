/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.tmp.MigratingSpaceConfigClient;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.admin.AdminMessage;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
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

  private static final Map<String, ConcurrentLinkedQueue<Handler<AsyncResult<Space>>>> pendingHandlers = new ConcurrentHashMap<>();
  private SpaceSelectionCondition emptySpaceCondition = new SpaceSelectionCondition();

  public static SpaceConfigClient getInstance() {
    if (Service.configuration.SPACES_DYNAMODB_TABLE_ARN != null) {
      if (Service.configuration.STORAGE_DB_URL != null) {
        //We're in the migration phase
        return MigratingSpaceConfigClient.getInstance();
      }
      else
          return new DynamoSpaceConfigClient(Service.configuration.SPACES_DYNAMODB_TABLE_ARN);
    }
    else
      return JDBCSpaceConfigClient.getInstance();
  }

  public void get(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    Space cached = cache.get(spaceId);
    if (cached != null) {
      logger.info(marker, "space[{}]: Loaded space with title \"{}\" from cache", spaceId, cached.getTitle());
      handler.handle(Future.succeededFuture(cached));
      return;
    }

    /*
    In case we get the query for a space of which a previous request is already in flight we wait for its response and call the callback
    then. This is a performance optimization for highly parallel requests coming from the user at once.
     */
    boolean isFirstRequest = pendingHandlers.putIfAbsent(spaceId, new ConcurrentLinkedQueue<>()) == null;
    pendingHandlers.get(spaceId).add(handler);
    if (!isFirstRequest) {
      return;
    }

    getSpace(marker, spaceId, ar -> {
      ConcurrentLinkedQueue<Handler<AsyncResult<Space>>> handlersToCall = pendingHandlers.remove(spaceId);
      if (ar.succeeded()) {
        Space space = ar.result();
        if (space != null) {
          cache.put(spaceId, space);
          logger.info(marker, "space[{}]: Loaded space with title: \"{}\"", spaceId, space.getTitle());
        } else {
          logger.info(marker, "space[{}]: Space with this ID was not found", spaceId);
        }
        cache.put(spaceId, space);
        handlersToCall.forEach(h -> h.handle(Future.succeededFuture(ar.result())));
      } else {
        logger.info(marker, "space[{}]: Failed to load the space, reason: {}", spaceId, ar.cause());
        handlersToCall.forEach(h -> h.handle(Future.failedFuture(ar.cause())));
      }
    });
  }

  public void store(Marker marker, Space space, Handler<AsyncResult<Space>> handler) {
    if (space.getId() == null) {
      space.setId(RandomStringUtils.randomAlphanumeric(10));
    }

    storeSpace(marker, space, ar -> {
      if (ar.succeeded()) {
        invalidateCache(space.getId());
        logger.info(marker, "space[{}]: Stored successfully with title: \"{}\"", space.getId(), space.getTitle());
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
        logger.info(marker, "space[{}]: Failed storing the space", space.getId(), ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void delete(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler) {
    deleteSpace(marker, spaceId, ar -> {
      if (ar.succeeded()) {
        invalidateCache(spaceId);
        logger.info(marker, "space[{}]: Deleted space", spaceId);
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
        logger.info(marker, "space[{}]: Failed deleting the space", spaceId, ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void getSelected(Marker marker, SpaceAuthorizationCondition authorizedCondition, SpaceSelectionCondition selectedCondition,
      Handler<AsyncResult<List<Space>>> handler) {
    getSelectedSpaces(marker, authorizedCondition, selectedCondition, ar -> {
      if (ar.succeeded()) {
        List<Space> spaces = ar.result();
        spaces.forEach(s -> cache.put(s.getId(), s));
        logger.info(marker, "Loaded spaces by condition");
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
        logger.info(marker, "Failed to load spaces by condition", ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  public void getOwn(Marker marker, String ownerId, Handler<AsyncResult<List<Space>>> handler) {
    SpaceSelectionCondition selectedCondition = new SpaceSelectionCondition();
    selectedCondition.ownerIds = Collections.singleton(ownerId);
    selectedCondition.shared = false;
    getSelected(marker, emptySpaceCondition, selectedCondition, handler);
  }

  protected abstract void getSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler);

  protected abstract void storeSpace(Marker marker, Space space, Handler<AsyncResult<Space>> handler);

  protected abstract void deleteSpace(Marker marker, String spaceId, Handler<AsyncResult<Space>> handler);

  protected abstract void getSelectedSpaces(Marker marker, SpaceAuthorizationCondition authorizedCondition,
      SpaceSelectionCondition selectedCondition, Handler<AsyncResult<List<Space>>> handler);

  public void invalidateCache(String spaceId) {
    cache.remove(spaceId);
    new InvalidateSpaceCacheMessage().withId(spaceId).broadcast();
  }

  public static class SpaceAuthorizationCondition {

    public Set<String> spaceIds;
    public Set<String> ownerIds;
    public Set<String> packages;
  }

  public static class SpaceSelectionCondition extends SpaceAuthorizationCondition {

    public boolean shared = true;
    public boolean negateOwnerIds = false;
  }

  public static class InvalidateSpaceCacheMessage extends AdminMessage {

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
    protected void handle() {
      cache.remove(id);
    }
  }
}
