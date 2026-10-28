/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.util.service;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.RequestOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BaseHttpServerVerticleTest {

  private Vertx vertx;
  private HttpServer server;
  private HttpClient client;

  @BeforeEach
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    server = vertx.createHttpServer(new HttpServerOptions(BaseHttpServerVerticle.SERVER_OPTIONS))
        .requestHandler(request -> request.response().end())
        .listen(0, "localhost")
        .toCompletionStage().toCompletableFuture().get(10, SECONDS);
    client = vertx.createHttpClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    vertx.close().toCompletionStage().toCompletableFuture().get(10, SECONDS);
  }

  @Test
  public void largeCookieIsAccepted() throws Exception {
    //More than the Vert.x default maxHeaderSize of 8192 bytes, which answers with 431
    assertEquals(200, sendWithCookieOfSize(12 * 1024));
  }

  @Test
  public void tooLargeHeadersAreRejected() throws Exception {
    assertEquals(431, sendWithCookieOfSize(17 * 1024));
  }

  private int sendWithCookieOfSize(int size) throws Exception {
    RequestOptions options = new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setHost("localhost")
        .setPort(server.actualPort())
        .setURI("/")
        .putHeader(HttpHeaders.COOKIE, "c=" + "x".repeat(size));
    return client.request(options)
        .compose(request -> request.send())
        .map(HttpClientResponse::statusCode)
        .toCompletionStage().toCompletableFuture().get(10, SECONDS);
  }
}
