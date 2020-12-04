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
package com.here.xyz.hub.rest;

import com.here.xyz.events.GetHistoryStatisticsEvent;
import com.here.xyz.events.IterateHistoryEvent;
import com.here.xyz.hub.task.FeatureTask;
import io.vertx.ext.web.ParsedHeaderValue;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_HERE_COMPACT_CHANGESET;
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;

public class HistoryQueryApi extends SpaceBasedApi{

    public HistoryQueryApi(OpenAPI3RouterFactory routerFactory) {
        routerFactory.addHandlerByOperationId("iterateHistory", this::iterateHistory);
        routerFactory.addHandlerByOperationId("getHistoryStatistics", this::getHistoryStatistics);
    }

    public void iterateHistory(final RoutingContext context) {
        try {
            final boolean skipCache = ApiParam.Query.getBoolean(context, SKIP_CACHE, false);

            ApiResponseType responseType = ApiResponseType.CHANGESET_COLLECTION;

            if( context.parsedHeaders().accept().stream().map(ParsedHeaderValue::rawValue).anyMatch( APPLICATION_VND_HERE_COMPACT_CHANGESET::equals) )
                responseType = ApiResponseType.COMPACT_CHANGESET;

            IterateHistoryEvent event = new IterateHistoryEvent()
                    .withCompact(responseType.equals(ApiResponseType.COMPACT_CHANGESET) ? true : false)
                    .withLimit(getLimit(context))
                    .withVStart(ApiParam.Query.getInteger(context, ApiParam.Query.VSTART, 00))
                    .withVEnd(ApiParam.Query.getInteger(context, ApiParam.Query.VEND, 0))
                    .withNextPageToken(ApiParam.Query.getString(context, ApiParam.Query.NEXT_PAGE_TOKEN, null));

            final FeatureTask.IterateHistoryQuery task = new FeatureTask.IterateHistoryQuery(event, context, responseType, skipCache);
            task.execute(this::sendResponse, this::sendErrorResponse);
        } catch (Exception e) {
            sendErrorResponse(context, e);
        }
    }

    public void getHistoryStatistics(final RoutingContext context) {
        new FeatureTask.GetHistoryStatistics(new GetHistoryStatisticsEvent(), context, ApiResponseType.HISTORY_STATISTICS_RESPONSE, ApiParam.Query.getBoolean(context, SKIP_CACHE, false))
                .execute(this::sendResponse, this::sendErrorResponse);
    }
}
