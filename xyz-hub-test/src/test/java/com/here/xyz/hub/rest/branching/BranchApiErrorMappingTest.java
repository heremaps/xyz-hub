/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.hub.rest.branching;

import com.here.xyz.hub.util.OpenApiTestUtil;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerInternal;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import org.junit.jupiter.api.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BranchApiErrorMappingTest {
    static Vertx vertx;
    static int port;
    static String SPEC;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        port = 20080 + (int)(Math.random() * 1000);
        SPEC = OpenApiTestUtil.sanitizeAndMaterialize(
                "xyz-hub-service/src/main/resources/openapi.yaml"
        );
        Promise<String> p = Promise.promise();
        vertx.deployVerticle(new ErrorStubVerticle(port, SPEC), p);
        p.future().toCompletionStage().toCompletableFuture().get();

        RestAssured.baseURI = "http://localhost";
        RestAssured.port = port;
    }

    static class ErrorStubVerticle extends AbstractVerticle {
        final int port;
        final String specPath;
        ErrorStubVerticle(int port, String specPath) { this.port = port; this.specPath = specPath; }

        @Override
        public void start(Promise<Void> startPromise) {
            RouterBuilder.create(vertx, specPath).onSuccess(rb -> {
                rb.setOptions(new RouterBuilderOptions().setRequireSecurityHandlers(false));

                AllowAllAuthHandler allowAll = new AllowAllAuthHandler();
                for (String scheme : OpenApiTestUtil.readSecuritySchemeNames(specPath)) {
                    rb.securityHandler(scheme, allowAll);
                }

                // Operations
                rb.operation("patchBranch").handler(ctx -> {
                    String branchId = ctx.pathParam("branchId");
                    var body = ctx.getBodyAsJson();
                    if ("missing".equals(branchId)) {
                        ctx.response().setStatusCode(404)
                                .putHeader("content-type", "application/json")
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"NotFound\"}");
                        return;
                    }
                    if (body != null && "conflict".equals(body.getString("baseRef"))) {
                        ctx.response().setStatusCode(400)
                                .putHeader("content-type", "application/json")
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"Conflict\",\"errorMessage\":\"Rebase conflict\"}");
                        return;
                    }
                    ctx.response().putHeader("content-type", "application/json")
                            .end("{\"id\":\"" + branchId + "\",\"baseRef\":\"main:42\"}");
                });

                rb.operation("execBranchOperation").handler(ctx -> {
                    var body = ctx.getBodyAsJson();
                    if (body == null || body.getString("type") == null) {
                        ctx.response().setStatusCode(400)
                                .putHeader("content-type", "application/json")
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"IllegalArgument\",\"errorMessage\":\"Missing type\"}");
                        return;
                    }
                    if ("Merge".equals(body.getString("type")) && "unknown".equals(body.getString("targetBranchId"))) {
                        ctx.response().setStatusCode(404)
                                .putHeader("content-type", "application/json")
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"NotFound\"}");
                        return;
                    }
                    ctx.response().putHeader("content-type", "application/json")
                            .end("{\"id\":\"myBranch\",\"baseRef\":\"main:11\",\"state\":\"MERGING\"}");
                });

                rb.operation("deleteBranch").handler(ctx -> {
                    String branchId = ctx.pathParam("branchId");
                    if ("missing".equals(branchId)) {
                        ctx.response().setStatusCode(404)
                                .putHeader("content-type", "application/json")
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"NotFound\",\"errorMessage\":\"Branch not found\"}");
                        return;
                    }
                    if ("protected".equals(branchId)) {
                        ctx.response().setStatusCode(400)
                                .putHeader("content-type", "application/json")
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"IllegalArgument\",\"errorMessage\":\"Cannot delete protected branch\"}");
                        return;
                    }
                    ctx.response().setStatusCode(200)
                            .putHeader("content-type", "application/json")
                            .end("{\"id\":\"" + branchId + "\",\"baseRef\":\"main:1\"}");
                });

                Router router;
                try {
                    router = rb.createRouter();
                } catch (Throwable t) {
                    startPromise.fail(t);
                    return;
                }

                Router root = Router.router(vertx);
                root.route("/hub/*").subRouter(router);

                vertx.createHttpServer()
                        .requestHandler(root)
                        .listen(port)
                        .onSuccess(s -> startPromise.complete())
                        .onFailure(startPromise::fail);

            }).onFailure(startPromise::fail);
        }

        static class AllowAllAuthHandler implements AuthenticationHandler, AuthenticationHandlerInternal {
            private final User user = User.create(new JsonObject().put("sub", "test"));

            @Override
            public void handle(RoutingContext ctx) {
                ctx.setUser(user);
                ctx.next();
            }

            @Override
            public void authenticate(RoutingContext ctx, Handler<AsyncResult<User>> handler) {
                handler.handle(Future.succeededFuture(user));
            }
        }
    }

    @Test
    void patchMissingBranchReturns404() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"baseRef\":\"main:2\"}")
                .when()
                .patch("/hub/spaces/s/branches/missing")
                .then()
                .statusCode(404);
    }

    @Test
    void patchRebaseConflictReturns400() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"baseRef\":\"conflict\"}")
                .when()
                .patch("/hub/spaces/s/branches/myBranch")
                .then()
                .statusCode(400)
                .body("errorMessage", containsString("conflict"));
    }

    @Test
    void missingTypeReturns400() {
        given()
                .contentType(ContentType.JSON)
                .body("{}")
                .when()
                .post("/hub/spaces/s/branches/myBranch")
                .then()
                .statusCode(400);
    }

    @Test
    void mergeUnknownTargetReturns404() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"type\":\"Merge\",\"targetBranchId\":\"unknown\"}")
                .when()
                .post("/hub/spaces/s/branches/myBranch")
                .then()
                .statusCode(404);
    }

    @Test
    void deleteMissingBranchReturns404() {
        given()
                .accept(ContentType.JSON)
                .when()
                .delete("/hub/spaces/s/branches/missing")
                .then()
                .statusCode(404);
    }

    @Test
    void deleteProtectedBranchReturns400() {
        given()
                .accept(ContentType.JSON)
                .when()
                .delete("/hub/spaces/s/branches/x")
                .then()
                .statusCode(400);
    }

    @Test
    void deleteOkReturns200AndBranch() {
        given()
                .accept(ContentType.JSON)
                .when()
                .delete("/hub/spaces/s/branches/ok")
                .then()
                .statusCode(200)
                .body("id", equalTo("ok"))
                .body("baseRef", equalTo("main:1"));
    }
}

