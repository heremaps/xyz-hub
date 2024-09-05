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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

public abstract class XyzWebClient {
  protected final String baseUrl;

  public XyzWebClient(String baseUrl) {this.baseUrl = baseUrl;}

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
    try {
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
      throw new WebClientException("Request was interrupted.", e);
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
    public ErrorResponseException(HttpResponse<byte[]> errorResponse) {
      super("Received error response with status code " + errorResponse.statusCode() + " response body:\n" + new String(errorResponse.body()));
      this.errorResponse = errorResponse;
    }

    public HttpResponse<byte[]> getErrorResponse() {
      return errorResponse;
    }
  }
}
