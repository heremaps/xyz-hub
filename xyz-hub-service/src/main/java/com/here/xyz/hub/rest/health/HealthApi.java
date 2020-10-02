/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.rest.health;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.admin.Node;
import com.here.xyz.hub.util.health.Config;
import com.here.xyz.hub.util.health.MainHealthCheck;
import com.here.xyz.hub.util.health.checks.ExecutableCheck;
import com.here.xyz.hub.util.health.checks.JDBCHealthCheck;
import com.here.xyz.hub.util.health.checks.MemoryHealthCheck;
import com.here.xyz.hub.util.health.checks.RedisHealthCheck;
import com.here.xyz.hub.util.health.checks.RemoteFunctionHealthAggregator;
import com.here.xyz.hub.util.health.schema.Reporter;
import com.here.xyz.hub.util.health.schema.Response;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HealthApi extends Api {

  private static final Logger logger = LogManager.getLogger();

  public static final String MAIN_HEALTCHECK_ENDPOINT = "/hub/";
  private static final URI NODE_HEALTHCHECK_ENDPOINT = getNodeHealthCheckEndpoint();
  private static MainHealthCheck healthCheck = new MainHealthCheck(true)
      .withReporter(
          new Reporter()
              .withVersion(Service.BUILD_VERSION)
              .withName("HERE XYZ Hub")
              .withBuildDate(Service.BUILD_TIME)
              .withUpSince(Service.START_TIME)
              .withEndpoint(getPublicServiceEndpoint())
      )
      .add(new RedisHealthCheck(Service.configuration.XYZ_HUB_REDIS_HOST, Service.configuration.XYZ_HUB_REDIS_PORT))
      .add(new RemoteFunctionHealthAggregator())
      .add(new MemoryHealthCheck());

  static {
    if (Service.configuration.STORAGE_DB_URL != null) {
      healthCheck.add(
          (ExecutableCheck) new JDBCHealthCheck(getStorageDbUri(), Service.configuration.STORAGE_DB_USER,
              Service.configuration.STORAGE_DB_PASSWORD)
              .withName("Configuration DB Postgres")
              .withEssential(true)
      );
    }
  }

  public HealthApi(Vertx vertx, Router router) {
    //The main health check endpoint
    router.route(HttpMethod.GET, MAIN_HEALTCHECK_ENDPOINT).handler(HealthApi::onHealthStatus);
    router.route(HttpMethod.GET, "/hub").handler(HealthApi::onHealthStatus);
    router.route(HttpMethod.GET, "/").handler(HealthApi::onHealthStatus); //TODO: Maybe better replace that one by a redirect to /hub/
    //Legacy:
    router.route(HttpMethod.GET, "/hub/health-status").handler(HealthApi::onHealthStatus);
  }

  private static URI getStorageDbUri() {
    try {
      return new URI(Service.configuration.STORAGE_DB_URL);
    } catch (URISyntaxException e) {
      logger.error("Wrong format of STORAGE_DB_URL: " + Service.configuration.STORAGE_DB_URL, e);
      return null;
    }
  }

  private static URI getPublicServiceEndpoint() {
    return URI.create(Service.configuration.XYZ_HUB_PUBLIC_ENDPOINT + MAIN_HEALTCHECK_ENDPOINT);
  }

  private static URI getNodeHealthCheckEndpoint() {
    try {
      return new URI("http://" + Service.getHostname() + ":" + Service.configuration.HTTP_PORT + MAIN_HEALTCHECK_ENDPOINT);
    } catch (URISyntaxException e) {
      logger.error("Wrong format of internal node hostname URI: " + Service.getHostname(), e);
      return null;
    }
  }

  public static void onHealthStatus(final RoutingContext context) {
    try {
      Response r = healthCheck.getResponse();
      r.setEndpoint(NODE_HEALTHCHECK_ENDPOINT);
      r.setNode(Node.OWN_INSTANCE.id);

      String secretHeaderValue = context.request().getHeader(Config.getHealthCheckHeaderName());

      //Always respond with 200 for public HC requests for now
      int statusCode = r.isPublicRequest(secretHeaderValue) ?
          OK.code() : r.getStatus().getSuggestedHTTPStatusCode();
      String responseString = r.toResponseString(secretHeaderValue);

      context.response().setStatusCode(statusCode)
          .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
          .end(responseString);
    }
    catch (Exception e) {
      logger.error(Context.getMarker(context), "Error while doing the health-check: ", e);
      context.response().setStatusCode(OK.code())
          .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
          .end(new JsonObject().put("status", new JsonObject().put("result", "WARNING")).encode());
    }
  }

  public static int getHealthStatusCode() {
    return healthCheck.getResponse().getStatus().getSuggestedHTTPStatusCode();
  }
}
