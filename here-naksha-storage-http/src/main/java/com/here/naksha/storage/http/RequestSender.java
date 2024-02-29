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
package com.here.naksha.storage.http;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static java.net.http.HttpRequest.newBuilder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RequestSender {

  private static final Logger log = LoggerFactory.getLogger(RequestSender.class);
  private final String hostUrl;
  private final Map<String, String> defaultHeaders;
  private final HttpClient httpClient;
  private final long socketTimeoutSec;

  public RequestSender(
      String hostUrl, Map<String, String> defaultHeaders, HttpClient httpClient, long socketTimeoutSec) {
    this.httpClient = httpClient;
    this.hostUrl = hostUrl;
    this.defaultHeaders = defaultHeaders;
    this.socketTimeoutSec = socketTimeoutSec;
  }

  /**
   * Send a request configured based on enclosing {@link HttpStorage}.
   *
   * @param endpoint does not contain host:port part, starts with "/".
   */
  HttpResponse<String> sendRequest(String endpoint) {
    return sendRequest(endpoint, true, null, null, null);
  }

  HttpResponse<String> sendRequest(
      @NotNull String endpoint,
      boolean keepDefHeaders,
      @Nullable Map<String, String> headers,
      @Nullable String httpMethod,
      @Nullable String body) {
    URI uri = URI.create(hostUrl + endpoint);
    HttpRequest.Builder builder = newBuilder().uri(uri).timeout(Duration.ofSeconds(socketTimeoutSec));

    if (keepDefHeaders) defaultHeaders.forEach(builder::header);
    if (headers != null) headers.forEach(builder::header);

    HttpRequest.BodyPublisher bodyPublisher =
        body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
    if (httpMethod != null) builder.method(httpMethod, bodyPublisher);
    HttpRequest request = builder.build();

    return sendRequest(request);
  }

  private HttpResponse<String> sendRequest(HttpRequest request) {
    try {
      long startTime = System.currentTimeMillis();
      CompletableFuture<HttpResponse<String>> futureResponse =
          httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
      HttpResponse<String> response = futureResponse.get(socketTimeoutSec, TimeUnit.SECONDS);
      long executionTime = System.currentTimeMillis() - startTime;
      log.info("Request to {} took {}ms", request.uri(), executionTime);
      return response;
    } catch (Exception e) {
      log.warn("We got exception while executing Http request against remote server.", e);
      throw unchecked(e);
    }
  }
}
