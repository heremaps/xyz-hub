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

import io.github.resilience4j.core.functions.CheckedFunction;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NakshaTestWebClient {

  private static final Logger logger = LoggerFactory.getLogger(NakshaTestWebClient.class);
  private static final String NAKSHA_HTTP_URI = "http://localhost:8080/";
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(10);

  private final HttpClient httpClient;

  private final RetryRegistry retryRegistry;

  public NakshaTestWebClient() {
    httpClient = HttpClient.newBuilder()
        .version(HTTP_1_1)
        .connectTimeout(CONNECT_TIMEOUT)
        .build();
    retryRegistry = configureRetryRegistry();
  }

  public HttpResponse<String> get(String subPath, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    HttpRequest getRequest = requestBuilder()
        .uri(nakshaPath(subPath))
        .GET()
        .header(HDR_STREAM_ID, streamId)
        .build();
    return sendOnce(getRequest);
  }

  public HttpResponse<String> post(String subPath, String jsonBody, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    HttpRequest postRequest = requestBuilder()
        .uri(nakshaPath(subPath))
        .POST(BodyPublishers.ofString(jsonBody))
        .header("Content-Type", "application/json")
        .header(HDR_STREAM_ID, streamId)
        .build();
    return sendOnce(postRequest);
  }

  public HttpResponse<String> put(String subPath, String jsonBody, String streamId)
      throws URISyntaxException, IOException, InterruptedException {
    HttpRequest putRequest = requestBuilder()
        .uri(nakshaPath(subPath))
        .PUT(BodyPublishers.ofString(jsonBody))
        .header("Content-Type", "application/json")
        .header(HDR_STREAM_ID, streamId)
        .build();
    return sendOnce(putRequest);
  }

  // TODO : Remove this function once JUnit pipeline has got multiple stable executions
  /**
   * This Http retry function was temporarily introduced as a workaround to resolve JUnit test hanging
   * issue, which is resolved now. This function will be removed soon.
   *
   * @param request http request to be submitted
   * @return actual http response
   * @deprecated use sendOnce() instead
   */
  private HttpResponse<String> send(HttpRequest request) {
    String retryId = retryIdForRequest(request);
    CheckedFunction<HttpRequest, HttpResponse<String>> responseSupplier =
        Retry.decorateCheckedFunction(retry(retryId), this::sendOnce);
    try {
      return responseSupplier.apply(request);
    } catch (Throwable e) {
      throw new RuntimeException("Applying retry (%s) failed".formatted(retryId), e);
    }
  }

  private HttpResponse<String> sendOnce(HttpRequest request) throws IOException, InterruptedException {
    logger.info("Sending {} request to {}", request.method(), request.uri());
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder requestBuilder() {
    return HttpRequest.newBuilder().version(Version.HTTP_1_1).timeout(SOCKET_TIMEOUT);
  }

  private URI nakshaPath(String subPath) throws URISyntaxException {
    return new URI(NAKSHA_HTTP_URI + subPath);
  }

  private Retry retry(String name) {
    Retry retry = retryRegistry.retry(name);
    retry.getEventPublisher().onRetry(ev -> logger.info("Retry triggereed: {}", name));
    return retry;
  }

  private static String retryIdForRequest(HttpRequest request) {
    return "%s_%s_retry".formatted(request.method(), request.uri().toString());
  }

  private static RetryRegistry configureRetryRegistry() {
    RetryConfig config = RetryConfig.custom()
        .maxAttempts(3)
        .retryExceptions(HttpTimeoutException.class)
        .build();
    return RetryRegistry.of(config);
  }
}
