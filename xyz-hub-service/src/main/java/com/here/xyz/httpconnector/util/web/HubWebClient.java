/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.xyz.httpconnector.util.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Export;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.client.HttpResponse;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HubWebClient {
    private static final Logger logger = LogManager.getLogger();

    private static ExpiringMap<String, Connector> connectorCache = ExpiringMap.builder()
        .expirationPolicy(ExpirationPolicy.CREATED)
        .expiration(3, TimeUnit.MINUTES)
        .build();

    public static Future<String> executeHTTPTrigger(Export job) {

        return CService.webClient.postAbs(CService.configuration.HUB_ENDPOINT
                        .substring(0,CService.configuration.HUB_ENDPOINT.lastIndexOf("/"))+"/_export-job")
                .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                .sendJson(job)
                .compose(res -> {
                    try {
                        if (res.statusCode() == HttpResponseStatus.NOT_FOUND.code())
                            throw new HttpException(HttpResponseStatus.NOT_FOUND, "TargetId does not exists!");

                        if (res.statusCode() != HttpResponseStatus.OK.code())
                            throw new Exception("Unexpected response code "+res.statusCode());

                        JsonObject resp = res.bodyAsJsonObject();
                        String id = resp.getString("id");

                        if (id == null)
                            throw new Exception("Id is missing!");

                        return Future.succeededFuture(id);
                    }
                    catch (Exception e){
                        logger.warn("job[{}] Unexpected HTTPTrigger response: {}! ", job.getId(), res.bodyAsString(), e);
                        return Future.failedFuture(e);
                    }
                });
    }

    public static Future<String> executeHTTPTriggerStatus(Export job) {
        String statusUrl = CService.configuration.HUB_ENDPOINT.substring(0,CService.configuration.HUB_ENDPOINT.lastIndexOf("/"))
                + "/_export-job-status";

        return CService.webClient.postAbs(statusUrl)
                .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                .sendJson(job)
                .compose(res -> {
                    try {
                        if (res.statusCode() == HttpResponseStatus.NOT_FOUND.code())
                            throw new HttpException(HttpResponseStatus.NOT_FOUND, "TargetId does not exists!");

                        if (res.statusCode() != HttpResponseStatus.OK.code())
                            throw new Exception("Unexpected response code " + res.statusCode());

                        JsonObject resp = res.bodyAsJsonObject();
                        String state = resp.getString("state");

                        if (state == null)
                            throw new Exception("State is missing!");

                        return Future.succeededFuture(state);
                    }
                    catch (Exception e){
                        logger.warn("job[{}] Unexpected HTTPTriggerStatus response! ", job.getId(), e);
                        return Future.failedFuture(e);
                    }
                });
    }

    public static Future<Job> performBaseLayerExport(String spaceId, Export job) {
        String executeUrl = CService.configuration.HUB_ENDPOINT+"/spaces/"+spaceId+"/jobs";

        return CService.webClient.postAbs(executeUrl)
                .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                .sendJson(job)
                .compose(res -> {
                    try {
                        if(res.statusCode() == HttpResponseStatus.CONFLICT.code()) {
                            //Job already present
                            return Future.succeededFuture(null);
                        }else if(res.statusCode() != HttpResponseStatus.CREATED.code()) {
                            return Future.failedFuture("Can't create Job!");
                        }

                        JsonObject resp = res.bodyAsJsonObject();
                        String id = resp.getString("id");
                        return Future.succeededFuture(id);
                    }
                    catch (Exception e){
                        logger.warn("job[{}] Unexpected response for creatJob! ", job.getId(), e);
                        return Future.failedFuture(e);
                    }
                })
                .compose(jobId -> {
                        /** Job is already Existing! */
                        if(jobId == null)
                            return Future.succeededFuture(job);

                        String startJobUrl = CService.configuration.HUB_ENDPOINT + "/spaces/" + spaceId + "/job/"+jobId+"/execute?command=start";
                        return CService.webClient.postAbs(startJobUrl)
                                .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                                .sendJson(job)
                                .compose(res -> {
                                        if (res.statusCode() != HttpResponseStatus.NO_CONTENT.code()
                                            && res.statusCode() != HttpResponseStatus.PRECONDITION_FAILED.code()) {
                                            return Future.failedFuture("Cant start Job!");
                                        }
                                        return Future.succeededFuture(job);
                                });
                    }
                );
    }

    public static Future<Connector> getConnectorConfig(String connectorId) {
      Connector cachedConnector = connectorCache.get(connectorId);
      if (cachedConnector != null)
        return Future.succeededFuture(cachedConnector);
      return CService.webClient.getAbs(CService.configuration.HUB_ENDPOINT+"/connectors/" + connectorId)
          .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
          .send()
          .compose(res -> {
              try {
                  Connector connector = DatabindCodec.mapper().convertValue(res.bodyAsJsonObject(), Connector.class);
                  return Future.succeededFuture(connector);
              }
              catch (Exception e) {
                  return Future.failedFuture("Can't get connector config!");
              }
          })
          .onFailure(f -> {
              Future.failedFuture("Can't get connector config!");
          });
    }

    public static Future<Void> updateSpaceConfig(JsonObject config, String spaceId) {
        //Update space-config
        config.put("contentUpdatedAt", CService.currentTimeMillis());

        return CService.webClient.patchAbs(CService.configuration.HUB_ENDPOINT+"/spaces/"+spaceId)
                .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                .sendJsonObject(config)
                .compose(res -> {
                    if(res.statusCode() != HttpResponseStatus.OK.code()) {
                        return Future.failedFuture("Cant patch Space!");
                    }
                    return Future.succeededFuture();
                });
    }

    public static Future<Space> getSpace(String spaceId) {
      return CService.webClient.getAbs(CService.configuration.HUB_ENDPOINT + "/spaces/" + spaceId)
          .send()
          .compose(response -> {
            try {
              return Future.succeededFuture(deserializeResponse(response, Space.class));
            }
            catch (Exception e) {
              return Future.failedFuture(e);
            }
          });
    }

  private static <T> T deserializeResponse(HttpResponse response, Class<T> klass) throws JsonProcessingException {
      return deserializeResponse(response.bodyAsString(), klass);
  }

    private static <T> T deserializeResponse(String responseBody, Class<T> klass) throws JsonProcessingException {
      T response = XyzSerializable.deserialize(responseBody, klass);
      if (response instanceof ErrorResponse)
        throw new RuntimeException(((ErrorResponse) response).getErrorMessage());
      return response;
    }

    public static Future<StatisticsResponse> getSpaceStatistics(String spaceId) {
        //Collect statistics from hub, which also ensures an existing table
        return CService.webClient.getAbs(CService.configuration.HUB_ENDPOINT + "/spaces/" + spaceId + "/statistics?skipCache=true")
                .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
                .send()
                .compose(res -> {
                    try {
                        Object response = XyzSerializable.deserialize(res.bodyAsString());
                        if (response instanceof StatisticsResponse) {
                            return Future.succeededFuture((StatisticsResponse)response);
                        } else
                            return Future.failedFuture("Cant get space statistics!");
                    } catch (JsonProcessingException e) {
                        return Future.failedFuture("Cant get space statistics!");
                    }
                })
                .onFailure(f -> {
                    Future.failedFuture("Cant get space statistics!");
                });
    }
}
