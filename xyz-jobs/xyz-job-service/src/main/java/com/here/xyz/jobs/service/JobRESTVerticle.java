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

package com.here.xyz.jobs.service;

import com.here.xyz.util.service.AbstractRouterBuilder;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobRESTVerticle extends BaseHttpServerVerticle {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void start(Promise<Void> startPromise) throws Exception {

        if (StringUtils.isEmpty(Config.instance.ROUTER_BUILDER_CLASS_NAMES)) {
            // Set default router if not provided in config
            Config.instance.ROUTER_BUILDER_CLASS_NAMES = JobRouter.class.getCanonicalName();
        }
        final List<String> routeBuilderClassList = Arrays.asList(Config.instance.ROUTER_BUILDER_CLASS_NAMES.split(","));

        final Router mainRouter = Router.router(vertx);
        List<Future> routeFutures = routeBuilderClassList.stream().map(r -> {
            try {
                Class<?> cls = Class.forName(r);
                AbstractRouterBuilder rb = (AbstractRouterBuilder) cls.newInstance();
                return rb.buildRoutes(vertx)
                    .compose(router -> {
                        super.addDefaultHandlers(router); //TODO: Why calling the super method and not the one below?
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
}
