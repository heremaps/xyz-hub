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
import com.here.xyz.responses.StatisticsResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.nio.charset.Charset;

public class HubWebClient {

    public static Future<Void> updateSpaceConfig(JsonObject config, String spaceId){
        /** Update space-config */
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

    public static Future<StatisticsResponse> getSpaceStatistics(String spaceId){
        /** Collect statistics from hub, which also ensures an existing table */
        return CService.webClient.getAbs(CService.configuration.HUB_ENDPOINT+"/spaces/"+spaceId+"/statistics?skipCache=true")
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
