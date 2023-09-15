/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.httpconnector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.httpconnector.rest.HttpConnectorApi;
import com.here.xyz.httpconnector.rest.HttpMaintenanceApi;
import com.here.xyz.httpconnector.rest.JobApi;
import com.here.xyz.httpconnector.rest.JobStatusApi;
import com.here.xyz.hub.AbstractHttpServerVerticle;
import com.here.xyz.hub.XYZHubRESTVerticle;
import com.here.xyz.psql.PSQLXyzConnector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;

/**
 * Verticle for HTTP-Connector. Includes three API tribes for :
 * - event Processing
 * - maintenance Tasks
 * - job execution
 */

public class PsqlHttpConnectorVerticle extends AbstractHttpServerVerticle {

  private static final Logger logger = LogManager.getLogger();
  private static Map<String, String> envMap;
  private AbstractConnectorHandler connector;

  private static String LOCATION = "openapi-http-connector.yaml";
  private static String API;

  static {
    try {
      final byte[] openapi = ByteStreams.toByteArray(XYZHubRESTVerticle.class.getResourceAsStream("/openapi-http-connector.yaml"));
      API = new String(openapi);
    } catch (Exception e) {
      logger.error("Unable to generate OpenApi specs.", e);
    }
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    connector = new PSQLXyzConnector();

    RouterBuilder.create(vertx, LOCATION).onComplete(ar -> {
      if (ar.succeeded()) {
        try {
          final RouterBuilder rb = ar.result();
          populateEnvMap();

          new HttpConnectorApi(rb, connector);
          new HttpMaintenanceApi(rb);
          new JobApi(rb);

          final Router router = rb.createRouter();

          new JobStatusApi(router);

          //OpenAPI resources
          router.route("/psql/static/openapi/*").handler(createCorsHandler()).handler((routingContext -> {
            final HttpServerResponse res = routingContext.response();
            res.putHeader("content-type", "application/yaml");
            final String path = routingContext.request().path();
            if (path.endsWith("openapi-http-connector.yaml")) {
              res.headers().add(CONTENT_LENGTH, String.valueOf(API.getBytes().length));
              res.write(API);
            }
            else {
              res.setStatusCode(HttpResponseStatus.NOT_FOUND.code());
            }
            res.end();
          }));

          //Add default handlers
          addDefaultHandlers(router);
          createHttpServer(CService.configuration.HTTP_PORT, router);
        }
        catch (Exception e) {
          logger.error("An error occurred, during the creation of the router from the Open API specification file.", e);
        }
      }
      else {
        logger.error("An error occurred, during the creation of the router from the Open API specification file.");
      }
    });
  }

  public static Map<String, String> getEnvMap() {
    if (envMap != null)
      return envMap;

    populateEnvMap();

    return envMap;
  }

  public static synchronized void populateEnvMap(){
    try {
      envMap = new ObjectMapper().convertValue(CService.configuration, HashMap.class);
    }catch (Exception e){
      logger.error("Cannot populate EnvMap");
    }
  }
}