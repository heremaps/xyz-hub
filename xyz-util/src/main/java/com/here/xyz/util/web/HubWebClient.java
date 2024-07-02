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

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.here.xyz.XyzSerializable.deserialize;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

public class HubWebClient extends XyzWebClient {
  private static Map<String, HubWebClient> instances = new HashMap<>();
  private ExpiringMap<String, Connector> connectorCache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(3, TimeUnit.MINUTES)
      .build();

  protected HubWebClient(String baseUrl) {
    super(baseUrl);
  }

  @Override
  public boolean isServiceReachable() {
    try {
      request(HttpRequest.newBuilder()
          .uri(uri("/"))
          .timeout(Duration.of(3, SECONDS)));
    }
    catch (WebClientException e) {
      return false;
    }
    return true;
  }

  public static HubWebClient getInstance(String baseUrl) {
    if (!instances.containsKey(baseUrl))
      instances.put(baseUrl, new HubWebClient(baseUrl));
    return instances.get(baseUrl);
  }

  public Space loadSpace(String spaceId) throws WebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId))).body(), Space.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public Space createSpace(String spaceId, String title) throws WebClientException {
    return createSpace(spaceId, title, null);
  }

  public Space createSpace(String spaceId, String title, Map<String, Object> spaceConfig) throws WebClientException {
    spaceConfig = new HashMap<>(spaceConfig == null ? Map.of() : spaceConfig);
    spaceConfig.put("id", spaceId);
    spaceConfig.put("title", title);

    try {
      return deserialize(request(HttpRequest.newBuilder()
              .uri(uri("/spaces"))
              .header(CONTENT_TYPE, JSON_UTF_8.toString())
              .method("POST", BodyPublishers.ofByteArray(XyzSerializable.serialize(spaceConfig).getBytes()))).body(), Space.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public void patchSpace(String spaceId, Map<String, Object> spaceUpdates) throws WebClientException {
    request(HttpRequest.newBuilder()
        .uri(uri("/spaces/" + spaceId))
        .header(CONTENT_TYPE, JSON_UTF_8.toString())
        .method("PATCH", BodyPublishers.ofByteArray(XyzSerializable.serialize(spaceUpdates).getBytes())));
  }

  public void deleteSpace(String spaceId) throws WebClientException {
    request(HttpRequest.newBuilder()
            .DELETE()
            .uri(uri("/spaces/" + spaceId)));
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws WebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId + "/statistics" + (context == null ? "" : "?context=" + context)))).body(), StatisticsResponse.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId) throws WebClientException {
    return loadSpaceStatistics(spaceId, null);
  }

  public Connector loadConnector(String connectorId) throws WebClientException {
    Connector cachedConnector = connectorCache.get(connectorId);
    if (cachedConnector != null)
      return cachedConnector;
    try {
      Connector connector = deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/connectors/" + connectorId))).body(), Connector.class);
      connectorCache.put(connectorId, connector);
      return connector;
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public List<Connector> loadConnectors() throws WebClientException {
    //TODO: Add caching also here
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/connectors"))).body(), new TypeReference<>() {});
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public Tag postTag(String spaceId, Tag tag) throws WebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId + "/tags"))
          .header(CONTENT_TYPE, JSON_UTF_8.toString())
          .method("POST", BodyPublishers.ofByteArray(tag.serialize().getBytes()))).body(), Tag.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public void deleteTag(String spaceId, String tagId) throws WebClientException {
    request(HttpRequest.newBuilder()
        .DELETE()
        .uri(uri("/spaces/" + spaceId + "/tags/" + tagId ))
        .header(CONTENT_TYPE, JSON_UTF_8.toString()));
  }

  public Tag loadTag(String spaceId, String tagId) throws WebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .GET()
          .uri(uri("/spaces/" + spaceId + "/tags/" + tagId))).body(), Tag.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }
}
