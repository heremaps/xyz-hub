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

import static java.net.http.HttpClient.Redirect.NORMAL;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.responses.ErrorResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public abstract class XyzWebClient {
  protected final String baseUrl;
  private final String userAgent;
  private final Map<String, String> extraHeaders;
  private static final int MAX_REQUEST_ATTEMPTS = 3;
  public static final String DEFAULT_USER_AGENT = "Unknown/0.0.0";

  protected XyzWebClient(String baseUrl, String userAgent) {
    this(baseUrl, userAgent, null);
  }

  protected XyzWebClient(String baseUrl, String userAgent, Map<String, String> extraHeaders) {
    this.baseUrl = baseUrl;
    this.userAgent = userAgent != null ? userAgent : DEFAULT_USER_AGENT;
    this.extraHeaders = extraHeaders;
  }

  protected final URI uri(String path) {
    return URI.create(baseUrl + path);
  }

  private HttpClient client() {
    HttpClient.Builder builder = HttpClient.newBuilder()
        .followRedirects(NORMAL)
        .connectTimeout(Duration.of(10, SECONDS));

    //Use HTTP/1.1 protocol for HTTP request
    if (baseUrl != null && baseUrl.startsWith("http://"))
      builder.version(Version.HTTP_1_1);

    return builder.build();
  }

  protected HttpResponse<byte[]> request(HttpRequest.Builder requestBuilder) throws WebClientException {
    return request(requestBuilder, 1);
  }

  private HttpResponse<byte[]> request(Builder requestBuilder, int attempt) throws WebClientException {
    try {
      if (extraHeaders != null)
        extraHeaders.entrySet().forEach(entry -> requestBuilder.header(entry.getKey(), entry.getValue()));
      requestBuilder.header("User-Agent", userAgent);

      HttpRequest request = requestBuilder.build();
      HttpResponse<byte[]> response = client().send(request, BodyHandlers.ofByteArray());
      if (response.statusCode() >= 400)
        throw new ErrorResponseException(response);
      return response;
    }
    catch (IOException e) {
      throw new WebClientException("Error sending the request or receiving the response", e);
    }
    catch (InterruptedException e) {
      if (attempt >= MAX_REQUEST_ATTEMPTS)
        throw new WebClientException("Request was interrupted.", e);
      return request(requestBuilder, attempt + 1);
    }
    catch (ErrorResponseException e) {
      List<Integer> retryableStatusCodes = List.of(429, 502, 503, 504);
      if (attempt >= MAX_REQUEST_ATTEMPTS || !retryableStatusCodes.contains(e.getStatusCode()))
        throw e;
      try {
        Thread.sleep((long) (Math.pow(2, attempt) * 1000));
      }
      catch (InterruptedException ignored) {}
      return request(requestBuilder, attempt + 1);
    }
  }

  public abstract boolean isServiceReachable();

  public static class WebClientException extends Exception {
    public WebClientException(String message) {
      super(message);
    }

    public WebClientException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public static class ErrorResponseException extends WebClientException {
    private HttpResponse<byte[]> errorResponse;
    private ErrorResponse parsedErrorResponse;
    private int statusCode;

    public ErrorResponseException(HttpResponse<byte[]> errorResponse) {
      super("Received error response with status code " + errorResponse.statusCode() + " response body:\n" + new String(errorResponse.body()));
      this.errorResponse = errorResponse;
      statusCode = errorResponse.statusCode();
      try {
        parsedErrorResponse = XyzSerializable.deserialize(errorResponse.body());
      }
      catch (JsonProcessingException ignored) {}
    }

    public HttpResponse<byte[]> getErrorResponse() {
      return errorResponse;
    }

    public ErrorResponse getParsedErrorResponse() {
      return parsedErrorResponse;
    }

    public int getStatusCode() {
      return statusCode;
    }
  }
}
