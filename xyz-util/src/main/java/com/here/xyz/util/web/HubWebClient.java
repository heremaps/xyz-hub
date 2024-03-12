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
import static java.net.http.HttpClient.Redirect.NORMAL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;

public class HubWebClient {
  private static Map<String, HubWebClient> instances = new HashMap<>();
  private final String baseUrl;
  private ExpiringMap<String, Connector> connectorCache = ExpiringMap.builder()
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(3, TimeUnit.MINUTES)
      .build();

  protected HubWebClient(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public static HubWebClient getInstance(String baseUrl) {
    if (!instances.containsKey(baseUrl))
      instances.put(baseUrl, new HubWebClient(baseUrl));
    return instances.get(baseUrl);
  }

  public Space loadSpace(String spaceId) throws HubWebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId))
          .build()).body(), Space.class);
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  public Space createSpace(String spaceId, String title) throws HubWebClientException {
    return createSpace(spaceId, title, null);
  }

  public Space createSpace(String spaceId, String title, Map<String, Object> spaceConfig) throws HubWebClientException {
    spaceConfig = new HashMap<>(spaceConfig == null ? Map.of() : spaceConfig);
    spaceConfig.put("id", spaceId);
    spaceConfig.put("title", title);

    try {
      return deserialize(request(HttpRequest.newBuilder()
              .uri(uri("/spaces"))
              .header(CONTENT_TYPE, JSON_UTF_8.toString())
              .method("POST", BodyPublishers.ofByteArray(XyzSerializable.serialize(spaceConfig).getBytes()))
              .build()).body(), Space.class);
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  public void patchSpace(String spaceId, Map<String, Object> spaceUpdates) throws HubWebClientException {
    request(HttpRequest.newBuilder()
        .uri(uri("/spaces/" + spaceId))
        .header(CONTENT_TYPE, JSON_UTF_8.toString())
        .method("PATCH", BodyPublishers.ofByteArray(XyzSerializable.serialize(spaceUpdates).getBytes()))
        .build());
  }

  public void deleteSpace(String spaceId) throws HubWebClientException {
    request(HttpRequest.newBuilder()
            .DELETE()
            .uri(uri("/spaces/" + spaceId))
            .build());
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId, SpaceContext context) throws HubWebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId + "/statistics" + (context == null ? "" : "?context=" + context)))
          .build()).body(), StatisticsResponse.class);
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  public StatisticsResponse loadSpaceStatistics(String spaceId) throws HubWebClientException {
    return loadSpaceStatistics(spaceId, null);
  }

  public Connector loadConnector(String connectorId) throws HubWebClientException {
    Connector cachedConnector = connectorCache.get(connectorId);
    if (cachedConnector != null)
      return cachedConnector;
    try {
      Connector connector = deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/connectors/" + connectorId))
          .build()).body(), Connector.class);
      connectorCache.put(connectorId, connector);
      return connector;
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  public List<Connector> loadConnectors() throws HubWebClientException {
    //TODO: Add caching also here
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/connectors"))
          .build()).body(), new TypeReference<>() {});
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  public Tag postTag(String spaceId, Tag tag) throws HubWebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .uri(uri("/spaces/" + spaceId + "/tags"))
          .header(CONTENT_TYPE, JSON_UTF_8.toString())
          .method("POST", BodyPublishers.ofByteArray(tag.serialize().getBytes()))
          .build()).body(), Tag.class);
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  public void deleteTag(String spaceId, String tagId) throws HubWebClientException {
    request(HttpRequest.newBuilder()
        .DELETE()
        .uri(uri("/spaces/" + spaceId + "/tags/" + tagId ))
        .header(CONTENT_TYPE, JSON_UTF_8.toString())
        .build());
  }

  public Tag loadTag(String spaceId, String tagId) throws HubWebClientException {
    try {
      return deserialize(request(HttpRequest.newBuilder()
          .GET()
          .uri(uri("/spaces/" + spaceId + "/tags/" + tagId))
          .build()).body(), Tag.class);
    }
    catch (JsonProcessingException e) {
      throw new HubWebClientException("Error deserializing response", e);
    }
  }

  private URI uri(String path) {
    return URI.create(baseUrl + path);
  }

  private HttpClient client() {
    return HttpClient.newBuilder().followRedirects(NORMAL).build();
  }

  private HttpResponse<byte[]> request(HttpRequest request) throws HubWebClientException {
    try {
      HttpResponse<byte[]> response = client().send(request, BodyHandlers.ofByteArray());
      if (response.statusCode() >= 400)
        throw new ErrorResponseException("Received error response with status code: " + response.statusCode(), response);
      return response;
    }
    catch (IOException e) {
      throw new HubWebClientException("Error sending the request to hub or receiving the response", e);
    }
    catch (InterruptedException e) {
      throw new HubWebClientException("Request was interrupted.", e);
    }
  }

  public static class HubWebClientException extends Exception {

    public HubWebClientException(String message) {
      super(message);
    }

    public HubWebClientException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static class ErrorResponseException extends HubWebClientException {
    private HttpResponse<byte[]> errorResponse;
    public ErrorResponseException(String message, HttpResponse<byte[]> errorResponse) {
      super(message);
      this.errorResponse = errorResponse;
    }

    public HttpResponse<byte[]> getErrorResponse() {
      return errorResponse;
    }
  }
}
