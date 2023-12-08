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
package com.here.naksha.app.service.http.tasks;

import static com.here.naksha.lib.core.NakshaAdminCollection.EVENT_HANDLERS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.core.view.ViewDeserialize;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandlerApiTask<T extends XyzResponse> extends AbstractApiTask<XyzResponse> {

  private static final String HANDLER_ID_PATH_KEY = "handlerId";

  private static final Logger logger = LoggerFactory.getLogger(EventHandlerApiTask.class);

  private final @NotNull EventHandlerApiReqType reqType;

  public EventHandlerApiTask(
      final @NotNull EventHandlerApiReqType reqType,
      final @NotNull NakshaHttpVerticle verticle,
      final @NotNull INaksha nakshaHub,
      final @NotNull RoutingContext routingContext,
      final @NotNull NakshaContext nakshaContext) {
    super(verticle, nakshaHub, routingContext, nakshaContext);
    this.reqType = reqType;
  }

  public enum EventHandlerApiReqType {
    GET_ALL_HANDLERS,
    GET_HANDLER_BY_ID,
    CREATE_HANDLER,
    UPDATE_HANDLER,
    DELETE_HANDLER
  }

  @Override
  protected void init() {}

  @Override
  protected @NotNull XyzResponse execute() {
    try {
      return switch (reqType) {
        case CREATE_HANDLER -> executeCreateHandler();
        case GET_ALL_HANDLERS -> executeGetHandlers();
        case GET_HANDLER_BY_ID -> executeGetHandlerById();
        case UPDATE_HANDLER -> executeUpdateHandler();
        default -> executeUnsupported();
      };
    } catch (Exception ex) {
      // unexpected exception
      return verticle.sendErrorResponse(
          routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
    }
  }

  private @NotNull XyzResponse executeCreateHandler() throws Exception {
    // Read request JSON
    final EventHandler newHandler = handlerFromRequestBody();
    final WriteXyzFeatures writeRequest = RequestHelper.createFeatureRequest(EVENT_HANDLERS, newHandler, false);
    // persist new handler in Admin DB (if doesn't exist already)
    try (Result writeResult = executeWriteRequestFromSpaceStorage(writeRequest)) {
      return transformWriteResultToXyzFeatureResponse(writeResult, EventHandler.class);
    }
  }

  private @NotNull XyzResponse executeGetHandlers() {
    // Create ReadFeatures Request to read all handlers from Admin DB
    final ReadFeatures request = new ReadFeatures(EVENT_HANDLERS);
    // Submit request to NH Space Storage
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      // transform ReadResult to Http FeatureCollection response
      return transformReadResultToXyzCollectionResponse(rdResult, EventHandler.class);
    }
  }

  private @NotNull XyzResponse executeGetHandlerById() {
    // Create ReadFeatures Request to read the handler with the specific ID from Admin DB
    final String handlerId = routingContext.pathParam(HANDLER_ID_PATH_KEY);
    final ReadFeatures request = new ReadFeatures(EVENT_HANDLERS).withPropertyOp(POp.eq(PRef.id(), handlerId));
    // Submit request to NH Space Storage
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      return transformReadResultToXyzFeatureResponse(rdResult, EventHandler.class);
    }
  }

  private @NotNull XyzResponse executeUpdateHandler() throws JsonProcessingException {
    String handlerIdFromPath = routingContext.pathParam(HANDLER_ID_PATH_KEY);
    EventHandler handlerToUpdate = handlerFromRequestBody();
    if (!handlerIdFromPath.equals(handlerToUpdate.getId())) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, mismatchMsg(handlerIdFromPath, handlerToUpdate));
    } else {
      final WriteXyzFeatures updateHandlerReq =
          RequestHelper.updateFeatureRequest(EVENT_HANDLERS, handlerToUpdate);
      try (Result updateHandlerResult = executeWriteRequestFromSpaceStorage(updateHandlerReq)) {
        return transformWriteResultToXyzFeatureResponse(updateHandlerResult, EventHandler.class);
      }
    }
  }

  private @NotNull EventHandler handlerFromRequestBody() throws JsonProcessingException {
    try (final Json json = Json.get()) {
      final String bodyJson = routingContext.body().asString();
      return json.reader(ViewDeserialize.User.class)
          .forType(EventHandler.class)
          .readValue(bodyJson);
    }
  }

  private static String mismatchMsg(String handlerIdFromPath, EventHandler handlerToUpdate) {
    return "Mismatch between event handler ids. Path event handler id: %s, body event handler id: %s"
        .formatted(handlerIdFromPath, handlerToUpdate.getId());
  }
}
