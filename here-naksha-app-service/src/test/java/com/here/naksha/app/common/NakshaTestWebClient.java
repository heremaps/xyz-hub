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
package com.here.naksha.app.common;

import static com.here.naksha.app.common.TestUtil.HDR_STREAM_ID;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static org.apache.hc.core5.http.HttpHeaders.AUTHORIZATION;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NakshaTestWebClient {

  private static final boolean ALLOW_DEBUG_SPECIFIC_TIMEOUT = true;

  private static final Logger logger = LoggerFactory.getLogger(NakshaTestWebClient.class);

  private static final long DEBUG_SOCKET_TIMEOUT_SEC = 300;

  private static final long DEFAULT_NON_DEBUG_SOCKET_TIMEOUT_SEC = 5;

  private final String NAKSHA_HTTP_URI;
  private final Duration SOCKET_TIMEOUT;
  private final HttpClient httpClient;

  public NakshaTestWebClient(final @NotNull String nhUrl, final long connectTimeoutSec, final long socketTimeoutSec) {
    NAKSHA_HTTP_URI = nhUrl;
    SOCKET_TIMEOUT = Duration.ofSeconds(socketTimeoutSec);
    httpClient = HttpClient.newBuilder()
        .version(HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(connectTimeoutSec))
        .build();
  }

  public NakshaTestWebClient(long socketTimeoutSec) {
    this("http://localhost:8080/", 10, socketTimeoutSec);
  }

  public NakshaTestWebClient() {
    this(chooseSocketTimeout());
  }

  public HttpResponse<String> get(String subPath, String streamId, Duration timeout, @Nullable String jwt)
          throws URISyntaxException, IOException, InterruptedException {
    Builder requestBuilder = requestBuilder(timeout)
            .uri(nakshaPath(subPath))
            .GET()
            .header(HDR_STREAM_ID, streamId);
    if (jwt != null) {
      requestBuilder.header(AUTHORIZATION,jwt);
    }
    return sendOnce(requestBuilder.build());
  }

  public HttpResponse<String> get(String subPath, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    return this.get(subPath, streamId, SOCKET_TIMEOUT, null);
  }

  public HttpResponse<String> get(String subPath, String streamId, String jwt)
          throws URISyntaxException, IOException, InterruptedException {
    return this.get(subPath, streamId, SOCKET_TIMEOUT, jwt);
  }

  public HttpResponse<String> post(String subPath, String jsonBody, String streamId, Duration timeout, @Nullable String jwt)
          throws URISyntaxException, IOException, InterruptedException {
    Builder requestBuilder = requestBuilder(timeout)
            .uri(nakshaPath(subPath))
            .POST(BodyPublishers.ofString(jsonBody))
            .header("Content-Type", "application/json")
            .header(HDR_STREAM_ID, streamId);
    if (jwt != null) {
      requestBuilder.header(AUTHORIZATION,jwt);
    }
    return sendOnce(requestBuilder.build());
  }

  public HttpResponse<String> post(String subPath, String jsonBody, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    return this.post(subPath, jsonBody, streamId, SOCKET_TIMEOUT, null);
  }

  public HttpResponse<String> post(String subPath, String jsonBody, String streamId, String jwt)
          throws URISyntaxException, IOException, InterruptedException {
    return this.post(subPath, jsonBody, streamId, SOCKET_TIMEOUT, jwt);
  }

  public HttpResponse<String> post(String subPath, String jsonBody, String streamId, Duration timeout)
          throws URISyntaxException, IOException, InterruptedException {
    return this.post(subPath, jsonBody, streamId, timeout, null);
  }

  public HttpResponse<String> put(String subPath, String jsonBody, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    HttpRequest putRequest = requestBuilder(SOCKET_TIMEOUT)
        .uri(nakshaPath(subPath))
        .PUT(BodyPublishers.ofString(jsonBody))
        .header("Content-Type", "application/json")
        .header(HDR_STREAM_ID, streamId)
        .build();
    return sendOnce(putRequest);
  }

  public HttpResponse<String> options(String subPath, String originHeader, String requestMethod, String reqHeaderKeys)
          throws URISyntaxException, IOException, InterruptedException {
    HttpRequest optionsRequest = requestBuilder(SOCKET_TIMEOUT)
            .uri(nakshaPath(subPath))
            .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
            .header("Origin", originHeader)
            .header("Access-Control-Request-Method", requestMethod)
            .header("Access-Control-Request-Headers", reqHeaderKeys)
            .build();
    return sendOnce(optionsRequest);
  }

  public HttpResponse<String> patch(String subPath, String jsonBody, String streamId)
          throws URISyntaxException, IOException, InterruptedException {
    HttpRequest patchRequest = requestBuilder(SOCKET_TIMEOUT)
            .uri(nakshaPath(subPath))
            .method("PATCH",BodyPublishers.ofString(jsonBody))
            .header("Content-Type", "application/json")
            .header(HDR_STREAM_ID, streamId)
            .build();
    return sendOnce(patchRequest);
  }

  public HttpResponse<String> delete(String subPath, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    HttpRequest deleteRequest = requestBuilder(SOCKET_TIMEOUT)
        .uri(nakshaPath(subPath))
        .DELETE()
        .header("Content-Type", "application/json")
        .header(HDR_STREAM_ID, streamId)
        .build();
    return sendOnce(deleteRequest);
  }

  private HttpResponse<String> sendOnce(HttpRequest request) throws IOException, InterruptedException {
    logger.info("Sending {} request to {}", request.method(), request.uri());
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder requestBuilder(Duration soTimeout) {
    return HttpRequest.newBuilder().version(Version.HTTP_1_1).timeout(soTimeout);
  }

  private URI nakshaPath(String subPath) throws URISyntaxException {
    return new URI(NAKSHA_HTTP_URI + subPath);
  }

  private static long chooseSocketTimeout(){
    if(ALLOW_DEBUG_SPECIFIC_TIMEOUT && DebugDiscoverUtil.isAppRunningOnDebug()){
      return DEBUG_SOCKET_TIMEOUT_SEC;
    }
    return DEFAULT_NON_DEBUG_SOCKET_TIMEOUT_SEC;
  }
}
