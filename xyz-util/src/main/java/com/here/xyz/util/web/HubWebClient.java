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

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.GEO_JSON;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.here.xyz.XyzSerializable.deserialize;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzResponse;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

public class HubWebClient extends XyzWebClient {
  private static Map<InstanceKey, HubWebClient> instances = new ConcurrentHashMap<>();
  private ExpiringMap<String, Connector> connectorCache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(3, TimeUnit.MINUTES)
      .build();
  private ExpiringMap<String, StatisticsResponse> statisticsCache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(30, TimeUnit.SECONDS)
      .build();

  protected HubWebClient(String baseUrl) {
    super(baseUrl);
  }

  protected HubWebClient(String baseUrl, Map<String, String> extraHeaders) {
    super(baseUrl, extraHeaders);
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
    return getInstance(baseUrl, null);
  }

  public static HubWebClient getInstance(String baseUrl, Map<String, String> extraHeaders) {
    InstanceKey key = new InstanceKey(baseUrl, extraHeaders);
    if (!instances.containsKey(key))
      instances.put(key, new HubWebClient(baseUrl, extraHeaders));
    return instances.get(key);
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

  public String loadExtendedSpace(String spaceId) throws WebClientException {
    Space space = loadSpace(spaceId);
    if(space == null || space.getExtension() == null)
      return null;
    else
      return space.getExtension().getSpaceId();
  }

  public Space createSpace(String spaceId, String title) throws WebClientException {
    return createSpace(new Space().withId(spaceId).withTitle(title));
  }

  public Space createSpace(Space spaceConfig) throws WebClientException {
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

  public void putFeaturesWithoutResponse(String spaceId, FeatureCollection fc) throws WebClientException {
    request(HttpRequest.newBuilder()
        .uri(uri("/spaces/" + spaceId + "/features"))
        .header(CONTENT_TYPE, GEO_JSON.toString())
        .header(ACCEPT, "application/x-empty")
        .method("PUT", BodyPublishers.ofByteArray(XyzSerializable.serialize(fc).getBytes())));
  }

  public void deleteFeatures(String spaceId, List<String> featureIds) throws WebClientException {
    String idList = "?id="+String.join(",id=", featureIds);
    request(HttpRequest.newBuilder()
            .DELETE()
            .uri(uri("/spaces/" + spaceId + "/features" + idList))
            .header(CONTENT_TYPE, JSON_UTF_8.toString()));
  }

  public XyzResponse postFeatures(String spaceId, FeatureCollection fc, Map<String, String> queryParams) throws WebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
              .uri(uri("/spaces/" + spaceId + "/features?" + serializeQueryParams(queryParams)))
              .header(CONTENT_TYPE, GEO_JSON.toString())
              .method("POST", BodyPublishers.ofByteArray(XyzSerializable.serialize(fc).getBytes()))).body());
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  private String serializeQueryParams(Map<String, String> queryParams) {
    return queryParams == null ? "" : queryParams.entrySet().stream()
        .map(param -> param.getKey() + "=" + param.getValue())
        .collect(Collectors.joining("&"));
  }

  public void deleteSpace(String spaceId) throws WebClientException {
    request(HttpRequest.newBuilder()
            .DELETE()
            .uri(uri("/spaces/" + spaceId)));
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws WebClientException {
    return loadSpaceStatistics(spaceId, context, false);
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context, boolean skipCache) throws WebClientException {
    try {
      String cacheKey = spaceId + ":" + context;
      StatisticsResponse statistics = statisticsCache.get(cacheKey);
      if (statistics != null)
        return statistics;

      statistics = deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId + "/statistics?skipCache="+skipCache
                  + (context == null ? "" : "&context=" + context)))).body(), StatisticsResponse.class);
      statisticsCache.put(cacheKey, statistics);
      return statistics;
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public FeatureCollection getFeaturesFromSmallSpace(String spaceId, SpaceContext context, String propertyFilter, boolean force2D) throws WebClientException {
    try {
      String filters = "?" + (context != null ? "&context=" + context : "")
                      + (force2D ? "&force2D=" + force2D : "")
                      + (propertyFilter != null ? "&" + URLEncoder.encode(propertyFilter, StandardCharsets.UTF_8) : "");

      return deserialize(request(HttpRequest.newBuilder()
              .uri(uri("/spaces/" + spaceId + "/search" + filters ))).body(), FeatureCollection.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public FeatureCollection customReadFeaturesQuery(String spaceId, String customPath) throws WebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
              .uri(uri("/spaces/" + spaceId + "/"+ customPath))).body(), FeatureCollection.class);
    }
    catch (JsonProcessingException e) {
      throw new WebClientException("Error deserializing response", e);
    }
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId) throws WebClientException {
    return loadSpaceStatistics(spaceId, null, true);
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

  protected record InstanceKey(String baseUrl, Map<String, String> extraHeaders) {}
}
