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

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
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
    Router router = Router.router(vertx);
    router.route("/cors").handler(CorsHandler.create(".*")).handler(context -> context.response().end("OK"));
    router.route("/unexpected").handler(context -> context.fail(new RuntimeException("Unexpected failure")));
    router.route("/").handler(context -> context.response().end());
    router.route().failureHandler(new BaseHttpServerVerticle().createFailureHandler());
    server = vertx.createHttpServer(new HttpServerOptions(BaseHttpServerVerticle.SERVER_OPTIONS))
        .requestHandler(router)
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
    assertEquals(200, send("/", MultiMap.caseInsensitiveMultiMap().add(HttpHeaders.COOKIE, "c=" + "x".repeat(12 * 1024))).status);
  }

  @Test
  public void tooLargeHeadersAreRejected() throws Exception {
    assertEquals(431, send("/", MultiMap.caseInsensitiveMultiMap().add(HttpHeaders.COOKIE, "c=" + "x".repeat(17 * 1024))).status);
  }

  @Test
  public void invalidOriginIsRejectedWith403() throws Exception {
    Response response = send("/cors", MultiMap.caseInsensitiveMultiMap().add(HttpHeaders.ORIGIN, "invalid-origin"));
    assertEquals(403, response.status);
    assertEquals(403, new JsonObject(response.body).getInteger("statusCode"));
    assertEquals("CORS Rejected - Invalid origin", new JsonObject(response.body).getString("message"));
  }

  @Test
  public void validOriginIsAccepted() throws Exception {
    assertEquals(200, send("/cors", MultiMap.caseInsensitiveMultiMap().add(HttpHeaders.ORIGIN, "https://example.com")).status);
  }

  @Test
  public void failureWithoutStatusReturns500() throws Exception {
    assertEquals(500, send("/unexpected", MultiMap.caseInsensitiveMultiMap()).status);
  }

  private Response send(String path, MultiMap headers) throws Exception {
    RequestOptions options = new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setHost("localhost")
        .setPort(server.actualPort())
        .setURI(path)
        .setHeaders(headers);
    return client.request(options)
        .compose(request -> request.send())
        .compose(response -> response.body().map(body -> new Response(response.statusCode(), body.toString())))
        .toCompletionStage().toCompletableFuture().get(10, SECONDS);
  }

  private record Response(int status, String body) {}
}
