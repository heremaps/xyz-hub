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

import com.here.xyz.util.service.BaseHttpServerVerticle.RequestCancelledException;
import io.vertx.core.Handler;
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
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class BaseHttpServerVerticleTest {

  private Vertx vertx;
  private HttpServer server;
  private HttpClient client;

  @BeforeEach
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
    Router router = Router.router(vertx);
    Handler<RoutingContext> failureHandler = new BaseHttpServerVerticle().createFailureHandler();
    router.route("/cors").handler(CorsHandler.create(".*")).handler(context -> context.response().end("OK"));
    router.route("/unexpected").handler(context -> context.fail(new RuntimeException("Unexpected failure")));
    //Same handler order as BaseHttpServerVerticle#addDefaultHandlers
    router.route("/form").handler(CorsHandler.create(".*")).handler(BaseHttpServerVerticle.createBodyHandler())
        .handler(context -> context.response().end("OK"));
    router.route("/unavailable").handler(context -> context.fail(503, new RuntimeException("Backend unavailable")));
    router.route("/illegal-state").handler(context -> context.fail(new IllegalStateException()));
    router.route("/cancelled").handler(context -> context.fail(new RequestCancelledException()))
        .failureHandler(context -> {
          failureHandler.handle(context);
          //sendErrorResponse() sends nothing for a cancelled request, end the response here to make that visible
          if (!context.response().ended())
            context.response().end("No error response");
        });
    router.route("/").handler(context -> context.response().end());
    router.route().failureHandler(failureHandler);
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

  @ParameterizedTest
  @ValueSource(strings = {"application/x-www-form-urlencoded", "multipart/form-data; boundary=xyz"})
  public void getWithFormBodyIsRejectedWith405(String contentType) throws Exception {
    MultiMap headers = MultiMap.caseInsensitiveMultiMap().add(HttpHeaders.CONTENT_TYPE, contentType);
    Response response = send(HttpMethod.GET, "/form", headers, "a=bc");
    assertEquals(405, response.status);
    assertEquals(405, new JsonObject(response.body).getInteger("statusCode"));
    assertEquals("Request method must be one of POST, PUT, PATCH or DELETE to decode a multipart request",
        new JsonObject(response.body).getString("message"));
  }

  @Test
  public void failureWithExplicitStatusKeepsIt() throws Exception {
    Response response = send("/unavailable", MultiMap.caseInsensitiveMultiMap());
    assertEquals(503, response.status);
    assertEquals(503, new JsonObject(response.body).getInteger("statusCode"));
    assertEquals("Backend unavailable", new JsonObject(response.body).getString("message"));
  }

  @Test
  public void cancelledRequestGetsNoErrorResponse() throws Exception {
    Response response = send("/cancelled", MultiMap.caseInsensitiveMultiMap());
    assertEquals(200, response.status);
    assertEquals("No error response", response.body);
  }

  @Test
  public void illegalStateWithoutMessageReturns500() throws Exception {
    Response response = send("/illegal-state", MultiMap.caseInsensitiveMultiMap());
    assertEquals(500, response.status);
    assertEquals(500, new JsonObject(response.body).getInteger("statusCode"));
  }

  private Response send(String path, MultiMap headers) throws Exception {
    return send(HttpMethod.GET, path, headers, null);
  }

  private Response send(HttpMethod method, String path, MultiMap headers, String requestBody) throws Exception {
    RequestOptions options = new RequestOptions()
        .setMethod(method)
        .setHost("localhost")
        .setPort(server.actualPort())
        .setURI(path)
        .setHeaders(headers);
    return client.request(options)
        .compose(request -> requestBody == null ? request.send() : request.send(requestBody))
        .compose(response -> response.body().map(body -> new Response(response.statusCode(), body.toString())))
        .toCompletionStage().toCompletableFuture().get(10, SECONDS);
  }

  private record Response(int status, String body) {}
}
