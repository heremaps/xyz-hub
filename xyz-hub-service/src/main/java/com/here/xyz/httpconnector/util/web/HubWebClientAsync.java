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

package com.here.xyz.httpconnector.util.web;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.util.Async;
import io.vertx.core.Future;
import java.util.List;
import java.util.Map;

public class HubWebClientAsync {
  private static Async async = new Async(20, CService.vertx, HubWebClientAsync.class);

  public static Future<Space> loadSpace(String spaceId) {
    return async.run(() -> HubWebClient.loadSpace(spaceId));
  }

  public static Future<Void> patchSpace(String spaceId, Map<String, Object> spaceUpdates) {
    return async.run(() -> {
      HubWebClient.patchSpace(spaceId, spaceUpdates);
      return null;
    });
  }

  public static Future<StatisticsResponse> loadSpaceStatistics(String spaceId, SpaceContext context) {
    return async.run(() -> HubWebClient.loadSpaceStatistics(spaceId, context));
  }

  public static Future<StatisticsResponse> loadSpaceStatistics(String spaceId) {
    return loadSpaceStatistics(spaceId, null);
  }


  public static Future<Connector> loadConnector(String connectorId) {
    return async.run(() -> HubWebClient.loadConnector(connectorId));

  }

  public static Future<List<Connector>> loadConnectors() {
    return async.run(() -> HubWebClient.loadConnectors());
  }

  public static Future<Void> postTag(String spaceId, Tag tag) {
    return async.run(() -> {
      HubWebClient.postTag(spaceId, tag);
      return null;
    });
  }

  public static Future<Tag> deleteTag(String spaceId, String tagId) {
    return async.run(() -> HubWebClient.deleteTag(spaceId, tagId) );
  }

}
