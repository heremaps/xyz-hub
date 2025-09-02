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
import static org.hamcrest.Matchers.*;

public class BranchOperationApiTest {
    static Vertx vertx;
    static int port;
    static String SPEC;

    @BeforeAll
    static void start() throws Exception {
        vertx = Vertx.vertx();
        port = 21080 + (int)(Math.random() * 1000);
        SPEC = OpenApiTestUtil.sanitizeAndMaterialize(
                "xyz-hub-service/src/main/resources/openapi.yaml"
        );
        Promise<String> p = Promise.promise();
        vertx.deployVerticle(new OpsStubVerticle(port, SPEC), p);
        p.future().toCompletionStage().toCompletableFuture().get();

        RestAssured.baseURI = "http://localhost";
        RestAssured.port = port;
    }

    static class OpsStubVerticle extends AbstractVerticle {
        final int port;
        final String specPath;
        OpsStubVerticle(int port, String specPath) { this.port = port; this.specPath = specPath; }

        @Override
        public void start(Promise<Void> startPromise) {
            RouterBuilder.create(vertx, specPath).onSuccess(rb -> {
                rb.setOptions(new RouterBuilderOptions().setRequireSecurityHandlers(false));

                AllowAllAuthHandler allowAll = new AllowAllAuthHandler();
                for (String scheme : com.here.xyz.hub.util.OpenApiTestUtil.readSecuritySchemeNames(specPath)) {
                    rb.securityHandler(scheme, allowAll);
                }

                // Operations
                rb.operation("execBranchOperation").handler(ctx -> {
                    var body = ctx.getBodyAsJson();
                    if (body == null || !"Merge".equals(body.getString("type")) || body.getString("targetBranchId") == null) {
                        ctx.response()
                                .setStatusCode(400)
                                .putHeader("content-type", "application/json")   // << add this
                                .end("{\"type\":\"ErrorResponse\",\"error\":\"IllegalArgument\",\"errorMessage\":\"Expect Merge + targetBranchId\"}");
                        return;
                    }
                    ctx.response()
                            .setStatusCode(200)
                            .putHeader("content-type", "application/json")
                            .end("{\"id\":\"myBranch\",\"baseRef\":\"main:11\",\"merges\":{\"27\":\"main:42\"}}");
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
    void mergeHappyPathReturns200AndBranch() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"type\":\"Merge\",\"targetBranchId\":\"main\"}")
                .when()
                .post("/hub/spaces/s/branches/myBranch")
                .then()
                .statusCode(200)
                .body("id", equalTo("myBranch"))
                .body("merges.27", equalTo("main:42"));
    }

    @Test
    void mergeMissingTargetReturns400() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"type\":\"Merge\"}")
                .when()
                .post("/hub/spaces/s/branches/myBranch")
                .then()
                .statusCode(400)
                .body("errorMessage", containsString("targetBranchId"));
    }

    @Test
    void wrongTypeReturns400() {
        given()
                .contentType(ContentType.JSON)
                .body("{\"type\":\"Rebase\",\"targetBranchId\":\"main\"}")
                .when()
                .post("/hub/spaces/s/branches/myBranch")
                .then()
                .statusCode(400);
    }
}

