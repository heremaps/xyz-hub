/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.util.web;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.Async;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HubWebClientAsync extends HubWebClient {
  private static Map<String, HubWebClientAsync> instances = new HashMap<>();
  private Async async;

  protected HubWebClientAsync(String baseUrl, Vertx vertx) {
    super(baseUrl);
    async = new Async(20, vertx, HubWebClientAsync.class);
  }

  public static HubWebClientAsync getInstance(String baseUrl, Vertx vertx) {
    if (!instances.containsKey(baseUrl))
      instances.put(baseUrl, new HubWebClientAsync(baseUrl, vertx));
    return instances.get(baseUrl);
  }

  public Future<Space> loadSpaceAsync(String spaceId) {
    return async.run(() -> loadSpace(spaceId));
  }

  public Future<Void> patchSpaceAsync(String spaceId, Map<String, Object> spaceUpdates) {
    return async.run(() -> {
      patchSpace(spaceId, spaceUpdates);
      return null;
    });
  }

  public Future<StatisticsResponse> loadSpaceStatisticsAsync(String spaceId, SpaceContext context) {
    return async.run(() -> loadSpaceStatistics(spaceId, context));
  }

  public Future<StatisticsResponse> loadSpaceStatisticsAsync(String spaceId) {
    return loadSpaceStatisticsAsync(spaceId, null);
  }

  public Future<Connector> loadConnectorAsync(String connectorId) {
    return async.run(() -> loadConnector(connectorId));
  }

  public Future<List<Connector>> loadConnectorsAsync() {
    return async.run(() -> loadConnectors());
  }

  public Future<Tag> postTagAsync(String spaceId, Tag tag) {
    return async.run(() -> postTag(spaceId, tag));
  }

  public Future<Void> deleteTagAsync(String spaceId, String tagId) {
    return async.run(() -> {
      deleteTag(spaceId, tagId);
      return null;
    });
  }
}
