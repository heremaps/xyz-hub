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

public class RequestSender {

  private static final Logger log = LoggerFactory.getLogger(RequestSender.class);

  @NotNull
  private final HttpClient httpClient;

  @NotNull
  private final RequestSender.KeyProperties keyProps;

  public RequestSender(@NotNull RequestSender.KeyProperties keyProps) {
    this.keyProps = keyProps;
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(keyProps.connectionTimeoutSec))
        .build();
  }

  /**
   * Send a request configured based on enclosing {@link HttpStorage}.
   *
   * @param endpoint does not contain host:port part, starts with "/".
   * @param addHeaders headers to be added to the ones defines {@link KeyProperties#defaultHeaders}.
   */
  HttpResponse<byte[]> sendRequest(@NotNull String endpoint, @Nullable Map<String, String> addHeaders) {
    return sendRequest(endpoint, true, addHeaders, null, null);
  }

  HttpResponse<byte[]> sendRequest(
      @NotNull String endpoint,
      boolean keepDefHeaders,
      @Nullable Map<String, String> headers,
      @Nullable String httpMethod,
      @Nullable String body) {
    URI uri = URI.create(keyProps.hostUrl + endpoint);
    HttpRequest.Builder builder = newBuilder().uri(uri).timeout(Duration.ofSeconds(keyProps.socketTimeoutSec));

    if (keepDefHeaders) keyProps.defaultHeaders.forEach(builder::header);
    if (headers != null) headers.forEach(builder::header);

    HttpRequest.BodyPublisher bodyPublisher =
        body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
    if (httpMethod != null) builder.method(httpMethod, bodyPublisher);
    HttpRequest request = builder.build();

    return sendRequest(request);
  }

  private HttpResponse<byte[]> sendRequest(HttpRequest request) {
    long startTime = System.currentTimeMillis();
    HttpResponse<byte[]> response = null;
    try {
      CompletableFuture<HttpResponse<byte[]>> futureResponse =
          httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
      response = futureResponse.get(keyProps.socketTimeoutSec, TimeUnit.SECONDS);
      return response;
    } catch (Exception e) {
      log.warn("We got exception while executing Http request against remote server.", e);
      throw unchecked(e);
    } finally {
      long executionTime = System.currentTimeMillis() - startTime;
      log.info(
          "[Storage API stats => type,storageId,host,method,path,status,timeTakenMs,resSize] - StorageAPIStats {} {} {} {} {} {} {} {}",
          "HttpStorage",
          keyProps.name,
          keyProps.hostUrl,
          request.method(),
          request.uri(),
          (response == null) ? "-" : response.statusCode(),
          executionTime,
          (response == null) ? 0 : response.body().length);
    }
  }

  public boolean hasKeyProps(KeyProperties thatKeyProps) {
    return this.keyProps.equals(thatKeyProps);
  }

  /**
   * Set of properties that are just enough to construct the sender
   * and distinguish unambiguously between objects
   * in terms of their effective configuration
   */
  public record KeyProperties(
      @NotNull String name,
      @NotNull String hostUrl,
      @NotNull Map<String, String> defaultHeaders,
      long connectionTimeoutSec,
      long socketTimeoutSec) {}
}
