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

package com.here.xyz.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.ByteStreams;
import com.here.xyz.jobs.rest.JobApi;
import com.here.xyz.util.openapi.OpenApiGenerator;
import com.here.xyz.util.service.AbstractRouterBuilder;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.openapi.router.OpenAPIRoute;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JobRESTVerticle extends BaseHttpServerVerticle implements AbstractRouterBuilder {

    private static final Logger logger = LogManager.getLogger();
    private static String CONTRACT_API;

    static {
        try {
            OpenApiGenerator.getValuesMap().put("SERVER_URL", Config.instance.HUB_ENDPOINT);
            final byte[] openapi = ByteStreams.toByteArray(JobRESTVerticle.class.getResourceAsStream("/openapi.yaml"));
            final byte[] recipes = ByteStreams.toByteArray(JobRESTVerticle.class.getResourceAsStream("/openapi-recipes.yaml"));
            CONTRACT_API = new String(OpenApiGenerator.generate(openapi, recipes, "contract"));
        } catch (Exception e) {
            logger.error("Unable to generate OpenApi specs.", e);
        }
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        if (StringUtils.isEmpty(Config.instance.ROUTER_BUILDER_CLASS_NAMES)) {
            Config.instance.ROUTER_BUILDER_CLASS_NAMES = JobRESTVerticle.class.getCanonicalName();
        }
        final List<String> routeBuilderClassList = Arrays.asList(Config.instance.ROUTER_BUILDER_CLASS_NAMES.split(","));

        final Router mainRouter = Router.router(vertx);
        List<Future> routeFutures = new ArrayList<>();

        routeFutures = routeBuilderClassList.stream().map(r -> {
            try {
                Class<?> cls = Class.forName(r);
                AbstractRouterBuilder rb = (AbstractRouterBuilder) cls.newInstance();
                return rb.buildRoutes(vertx)
                        .compose(router -> {
                            mainRouter.mountSubRouter("/", router);
                            return Future.succeededFuture();
                        });
            } catch (Exception e) {
                logger.error("Unable to build routes for {}", r);
                return Future.failedFuture(e);
            }
        }).collect(Collectors.toList());

        CompositeFuture.all(routeFutures).onSuccess(done -> {
            addDefaultHandlers(mainRouter);
            createHttpServer(Config.instance.HTTP_PORT, mainRouter)
                    .onSuccess(none -> startPromise.complete())
                    .onFailure(err -> startPromise.fail(err));
        }).onFailure(err -> startPromise.fail(err));

    }

    @Override
    protected void addDefaultHandlers(Router router) {
        super.addDefaultHandlers(router);
        router.route().last().handler(createNotFoundHandler());
    }
    @Override
    public Future<Router> buildRoutes(Vertx vertx) {
        ObjectMapper om = new ObjectMapper(new YAMLFactory());

        try {
            return OpenAPIContract.from(vertx, new JsonObject(om.readValue(CONTRACT_API, Map.class))).flatMap(contract -> {
                RouterBuilder rb = RouterBuilder.create(vertx, contract);

                for(OpenAPIRoute route : rb.getRoutes())
                    route.addHandler(BodyHandler.create());

                initializeJobApi(rb);

                final Router router = rb.createRouter();
                router.route(HttpMethod.GET, "/_jobs/health").handler(ctx -> ctx.response().send("OK"));
                router.route().order(1).handler(createCorsHandler());
                return Future.succeededFuture(router);
            });
        }
        catch (JsonProcessingException e) {
            return Future.failedFuture(e);
        }
    }

    private void initializeJobApi(RouterBuilder rb) {
        new JobApi(rb);
    }
}
