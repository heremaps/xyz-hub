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

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;

public class NakshaTestWebClient {

  private static final String NAKSHA_HTTP_URI = "http://localhost:8080/";
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(2);

  private final HttpClient httpClient;

  private final Retry retry;

  public NakshaTestWebClient() {
    httpClient = HttpClient.newBuilder()
        .version(HTTP_1_1)
        .connectTimeout(CONNECT_TIMEOUT)
        .build();
    retry = configureRetry();
  }

  public HttpResponse<String> get(String subPath, String streamId) throws URISyntaxException {
    HttpRequest getRequest = requestBuilder()
        .uri(nakshaPath(subPath))
        .GET()
        .header(HDR_STREAM_ID, streamId)
        .build();
    return send(getRequest);
  }

  public HttpResponse<String> post(String subPath, String jsonBody, String streamId) throws URISyntaxException {
    HttpRequest postRequest = requestBuilder()
        .uri(nakshaPath(subPath))
        .POST(BodyPublishers.ofString(jsonBody))
        .header("Content-Type", "application/json")
        .header(HDR_STREAM_ID, streamId)
        .build();
    return send(postRequest);
  }

  public HttpResponse<String> put(String subPath, String jsonBody, String streamId) throws URISyntaxException {
    HttpRequest putRequest = requestBuilder()
        .uri(nakshaPath(subPath))
        .PUT(BodyPublishers.ofString(jsonBody))
        .header("Content-Type", "application/json")
        .header(HDR_STREAM_ID, streamId)
        .build();
    return send(putRequest);
  }

  private HttpResponse<String> send(HttpRequest request) {
    return retry.executeSupplier(() -> {
      try {
        return sendOnce(request);
      } catch (IOException | InterruptedException e) {
        throw new RequestException(request, e);
      }
    });
  }

  private HttpResponse<String> sendOnce(HttpRequest request) throws IOException, InterruptedException {
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder requestBuilder() {
    return HttpRequest.newBuilder().version(Version.HTTP_1_1).timeout(SOCKET_TIMEOUT);
  }

  private URI nakshaPath(String subPath) throws URISyntaxException {
    return new URI(NAKSHA_HTTP_URI + subPath);
  }

  private static Retry configureRetry() {
    RetryConfig config = RetryConfig.custom()
        .maxAttempts(3)
        .retryExceptions(HttpTimeoutException.class)
        .build();
    RetryRegistry registry = RetryRegistry.of(config);
    return registry.retry("nakshaRequest");
  }

  static class RequestException extends RuntimeException {

    public RequestException(HttpRequest request, Throwable cause) {
      super(msg(request), cause);
    }

    private static String msg(HttpRequest request) {
      return "Request to Naksha failed, method: %s, uri: %s"
          .formatted(request.method(), request.uri().toString());
    }
  }
}
