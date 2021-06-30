/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub;

import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;

import com.google.common.io.ByteStreams;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.hub.rest.HttpConnectorApi;
import com.here.xyz.psql.PSQLXyzConnector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import java.util.HashMap;
import java.util.Map;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PsqlHttpVerticle extends AbstractHttpServerVerticle {

  private static final Logger logger = LogManager.getLogger();
  private static Map<String, String> envMap;
  private AbstractConnectorHandler connector;

  private static String LOCATION = "openapi-http-connector.yaml";
  private static String API;
  private static int HTTP_PORT;

  public static String ECPS_PASSPHRASE;
  public static Integer DB_INITIAL_POOL_SIZE;
  public static Integer DB_MIN_POOL_SIZE;
  public static Integer DB_MAX_POOL_SIZE;

  public static Integer DB_ACQUIRE_RETRY_ATTEMPTS;
  public static Integer DB_ACQUIRE_INCREMENT;

  public static Integer DB_CHECKOUT_TIMEOUT;
  public static boolean DB_TEST_CONNECTION_ON_CHECKOUT;

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

          new HttpConnectorApi(rb,connector);

          final Router router = rb.createRouter();

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
          createHttpServer(HTTP_PORT, router);
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

    try {
      populateEnvMap();
    }catch (Exception e){
      logger.error("Cannot populate EnvMap");
    }

    return envMap;
  }

  private static synchronized void populateEnvMap() throws Exception{
    envMap = new HashMap<>();
    HttpConnector.configuration.fieldNames().forEach(fieldName -> {
      Object value = HttpConnector.configuration.getValue(fieldName);
      if (value != null){
        envMap.put(fieldName, value.toString());
      }
    });

    ECPS_PASSPHRASE = (envMap.get("ECPS_PHRASE") == null ? "local" : envMap.get("ECPS_PHRASE")) ;
    HTTP_PORT = Integer.parseInt((envMap.get("HTTP_PORT") == null ? "9090" : envMap.get("HTTP_PORT")));

    DB_INITIAL_POOL_SIZE = Integer.parseInt((envMap.get("DB_INITIAL_POOL_SIZE") == null ? "5" : envMap.get("DB_INITIAL_POOL_SIZE")));
    DB_MIN_POOL_SIZE = Integer.parseInt((envMap.get("DB_MIN_POOL_SIZE") == null ? "1" : envMap.get("DB_MIN_POOL_SIZE")));
    DB_MAX_POOL_SIZE = Integer.parseInt((envMap.get("DB_MAX_POOL_SIZE") == null ? "50" : envMap.get("DB_MAX_POOL_SIZE")));

    DB_ACQUIRE_RETRY_ATTEMPTS = Integer.parseInt((envMap.get("DB_ACQUIRE_RETRY_ATTEMPTS") == null ? "10" : envMap.get("DB_ACQUIRE_RETRY_ATTEMPTS")));
    DB_ACQUIRE_INCREMENT = Integer.parseInt((envMap.get("DB_ACQUIRE_INCREMENT") == null ? "1" : envMap.get("DB_ACQUIRE_INCREMENT")));

    DB_CHECKOUT_TIMEOUT = Integer.parseInt((envMap.get("DB_CHECKOUT_TIMEOUT") == null ? "10" : envMap.get("DB_CHECKOUT_TIMEOUT")) )* 1000;
    DB_TEST_CONNECTION_ON_CHECKOUT = Boolean.parseBoolean((envMap.get("DB_TEST_CONNECTION_ON_CHECKOUT")));
  }
}