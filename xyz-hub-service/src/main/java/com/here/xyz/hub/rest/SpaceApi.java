/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.vertx.core.http.HttpHeaders.ACCEPT;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureTaskHandler.InvalidStorageException;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.ModifyOp.IfNotExists;
import com.here.xyz.hub.task.ModifySpaceOp;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.SpaceTask.MatrixReadQuery;
import com.here.xyz.hub.task.Task;
import com.here.xyz.models.hub.Space.Copyright;
import com.here.xyz.responses.ErrorResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import java.util.Collections;
import java.util.List;

public class SpaceApi extends Api {

  public SpaceApi(OpenAPI3RouterFactory routerFactory) {
    routerFactory.addHandlerByOperationId("getSpace", this::getSpace);
    routerFactory.addHandlerByOperationId("getSpaces", this::getSpaces);
    routerFactory.addHandlerByOperationId("postSpace", this::postSpace);
    routerFactory.addHandlerByOperationId("patchSpace", this::patchSpace);
    routerFactory.addHandlerByOperationId("deleteSpace", this::deleteSpace);
  }

  /**
   * Read a space.
   */
  public void getSpace(final RoutingContext context) {
    JsonObject input = new JsonObject().put("id", context.pathParam(ApiParam.Path.SPACE_ID));
    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input), IfNotExists.ERROR, IfExists.RETAIN, true);

    new ConditionalOperation(context, ApiResponseType.SPACE, modifyOp, true)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * List all spaces accessible for the provided credentials.
   */
  public void getSpaces(final RoutingContext context) {
    new MatrixReadQuery(
        context,
        ApiResponseType.SPACE_LIST,
        ApiParam.Query.getBoolean(context, ApiParam.Query.INCLUDE_RIGHTS, false),
        ApiParam.Query.getBoolean(context, Query.INCLUDE_CONNECTORS, false),
        ApiParam.Query.getString(context, ApiParam.Query.OWNER, MatrixReadQuery.ME)
    ).execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Create a new space.
   */
  public void postSpace(final RoutingContext context) {
    JsonObject input;
    try {
      input = context.getBodyAsJson();
    } catch (DecodeException e) {
      context.fail(new HttpException(BAD_REQUEST, "Invalid JSON string"));
      return;
    }
    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input), IfNotExists.CREATE, IfExists.ERROR,
        true);

    new ConditionalOperation(context, ApiResponseType.SPACE, modifyOp, false)
        .execute(this::sendResponse, this::sendErrorResponseOnEdit);
  }

  /**
   * Update a space.
   */
  public void patchSpace(final RoutingContext context) {
    JsonObject input;
    try {
      input = context.getBodyAsJson();
    } catch (DecodeException e) {
      context.fail(new HttpException(BAD_REQUEST, "Invalid JSON string"));
      return;
    }
    String pathId = context.pathParam(Path.SPACE_ID);

    if (input.getString("id") == null) {
      input.put("id", pathId);
    }
    if (!input.getString("id").equals(pathId)) {
      context.fail(
          new HttpException(BAD_REQUEST, "The space ID in the body does not match the ID in the resource path."));
      return;
    }

    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input), IfNotExists.ERROR, IfExists.PATCH, true);

    new ConditionalOperation(context, ApiResponseType.SPACE, modifyOp, true)
        .execute(this::sendResponse, this::sendErrorResponseOnEdit);

  }

  /**
   * Delete a space.
   */
  public void deleteSpace(final RoutingContext context) {
    JsonObject input = new JsonObject().put("id", context.pathParam(Path.SPACE_ID));
    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input), IfNotExists.ERROR, IfExists.DELETE, true);

    //Delete the space
    ApiResponseType responseType = APPLICATION_JSON.equals(context.request().getHeader(ACCEPT))
        ? ApiResponseType.SPACE : ApiResponseType.EMPTY;
    new ConditionalOperation(context, responseType, modifyOp, true)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Send an error response to the client when an exception occurred while processing a task.
   *
   * @param task the task for which to return an error response.
   * @param e the exception that should be used to generate an {@link ErrorResponse}, if null an internal server error is returned.
   */
  public void sendErrorResponseOnEdit(final Task task, final Exception e) {
    if (e instanceof InvalidStorageException) {
      sendErrorResponse(task.context, new HttpException(BAD_REQUEST, "The space contains an invalid storage ID."));
    } else {
      sendErrorResponse(task.context, e);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class BasicSpaceView {
    public String id;
    public String owner;
    public String title;
    public String description;
    @JsonInclude(Include.NON_DEFAULT)
    public List<Copyright> copyright;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RightsSpaceView extends BasicSpaceView {

    public List<String> rights;
  }
}
