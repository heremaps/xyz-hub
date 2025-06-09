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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HubWebClientAsync extends HubWebClient {
  private static Map<String, HubWebClientAsync> instances = new HashMap<>();
  private static final Async ASYNC = new Async(20, HubWebClientAsync.class);

  protected HubWebClientAsync(String baseUrl) {
    super(baseUrl);
  }

  public static HubWebClientAsync getInstance(String baseUrl) {
    if (!instances.containsKey(baseUrl))
      instances.put(baseUrl, new HubWebClientAsync(baseUrl));
    return instances.get(baseUrl);
  }

  public Future<Space> loadSpaceAsync(String spaceId) {
    return ASYNC.run(() -> loadSpace(spaceId));
  }

  public Future<Void> patchSpaceAsync(String spaceId, Map<String, Object> spaceUpdates) {
    return ASYNC.run(() -> {
      patchSpace(spaceId, spaceUpdates);
      return null;
    });
  }

  public Future<StatisticsResponse> loadSpaceStatisticsAsync(String spaceId, SpaceContext context, boolean skipCache, boolean fastMode) {
    return ASYNC.run(() -> loadSpaceStatistics(spaceId, context, skipCache, fastMode));
  }

  public Future<StatisticsResponse> loadSpaceStatisticsAsync(String spaceId) {
    return loadSpaceStatisticsAsync(spaceId, null, false, false);
  }

  public Future<Connector> loadConnectorAsync(String connectorId) {
    return ASYNC.run(() -> loadConnector(connectorId));
  }

  public Future<List<Connector>> loadConnectorsAsync() {
    return ASYNC.run(() -> loadConnectors());
  }

  public Future<Tag> postTagAsync(String spaceId, Tag tag) {
    return ASYNC.run(() -> postTag(spaceId, tag));
  }

  public Future<Tag> loadTagAsync(String spaceId, String tagId) {
    return ASYNC.run(() -> loadTag(spaceId, tagId));
  }

  public Future<Void> deleteTagAsync(String spaceId, String tagId) {
    return ASYNC.run(() -> {
      deleteTag(spaceId, tagId);
      return null;
    });
  }
}
