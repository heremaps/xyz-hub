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
package com.here.naksha.app.service.http.tasks;

import static com.here.naksha.app.service.http.ops.MaskingUtil.maskProperties;
import static com.here.naksha.common.http.apis.ApiParamsConst.HANDLER_ID;
import static com.here.naksha.lib.core.NakshaAdminCollection.EVENT_HANDLERS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.apis.ApiParams;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
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
        case DELETE_HANDLER -> executeDeleteHandler();
        default -> executeUnsupported();
      };
    } catch (Exception ex) {
      if (ex instanceof XyzErrorException xyz) {
        logger.warn("Known exception while processing request. ", ex);
        return verticle.sendErrorResponse(routingContext, xyz.xyzError, xyz.getMessage());
      } else {
        logger.error("Unexpected error while processing request. ", ex);
        return verticle.sendErrorResponse(
            routingContext, XyzError.EXCEPTION, "Internal error : " + ex.getMessage());
      }
    }
  }

  private @NotNull XyzResponse executeCreateHandler() throws Exception {
    // Read request JSON
    final EventHandler newHandler = handlerFromRequestBody();
    final WriteXyzFeatures writeRequest = RequestHelper.createFeatureRequest(EVENT_HANDLERS, newHandler, false);
    return transformResponseFor(writeRequest);
  }

  private @NotNull XyzResponse executeGetHandlers() {
    // Create ReadFeatures Request to read all handlers from Admin DB
    final ReadFeatures request = new ReadFeatures(EVENT_HANDLERS);
    // Submit request to NH Space Storage
    try (Result rdResult = executeReadRequestFromSpaceStorage(request)) {
      // transform ReadResult to Http FeatureCollection response
      return transformReadResultToXyzCollectionResponse(
          rdResult, EventHandler.class, this::handlerWithMaskedSensitiveProperties);
    }
  }

  private @NotNull XyzResponse executeGetHandlerById() {
    // Create ReadFeatures Request to read the handler with the specific ID from Admin DB
    final String handlerId = routingContext.pathParam(HANDLER_ID);
    final ReadFeatures request = new ReadFeatures(EVENT_HANDLERS).withPropertyOp(POp.eq(PRef.id(), handlerId));
    return transformResponseFor(request);
  }

  private @NotNull XyzResponse executeUpdateHandler() throws JsonProcessingException {
    String handlerIdFromPath = routingContext.pathParam(HANDLER_ID);
    EventHandler handlerToUpdate = handlerFromRequestBody();
    if (!handlerIdFromPath.equals(handlerToUpdate.getId())) {
      return verticle.sendErrorResponse(
          routingContext, XyzError.ILLEGAL_ARGUMENT, mismatchMsg(handlerIdFromPath, handlerToUpdate));
    } else {
      final WriteXyzFeatures updateHandlerReq =
          RequestHelper.updateFeatureRequest(EVENT_HANDLERS, handlerToUpdate);
      return transformResponseFor(updateHandlerReq);
    }
  }

  private @NotNull XyzResponse executeDeleteHandler() {
    final String handlerId = ApiParams.extractMandatoryPathParam(routingContext, HANDLER_ID);
    final WriteXyzFeatures wrRequest = RequestHelper.deleteFeatureByIdRequest(EVENT_HANDLERS, handlerId);
    try (Result wrResult = executeWriteRequestFromSpaceStorage(wrRequest)) {
      return transformDeleteResultToXyzFeatureResponse(
          wrResult, EventHandler.class, this::handlerWithMaskedSensitiveProperties);
    }
  }

  @NotNull
  private XyzResponse transformResponseFor(ReadFeatures rdRequest) {
    try (Result rdResult = executeReadRequestFromSpaceStorage(rdRequest)) {
      return transformReadResultToXyzFeatureResponse(
          rdResult, EventHandler.class, this::handlerWithMaskedSensitiveProperties);
    }
  }

  @NotNull
  private XyzResponse transformResponseFor(WriteXyzFeatures updateHandlerReq) {
    // persist new handler in Admin DB (if doesn't exist already)
    try (Result updateHandlerResult = executeWriteRequestFromSpaceStorage(updateHandlerReq)) {
      return transformWriteResultToXyzFeatureResponse(
          updateHandlerResult, EventHandler.class, this::handlerWithMaskedSensitiveProperties);
    }
  }

  private EventHandler handlerWithMaskedSensitiveProperties(EventHandler handler) {
    maskProperties(handler);
    return handler;
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
