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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.vertx.core.http.HttpHeaders.ACCEPT;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.ModifyOp.IfNotExists;
import com.here.xyz.hub.task.ModifySpaceOp;
import com.here.xyz.hub.task.SpaceTask.ConditionalOperation;
import com.here.xyz.hub.task.SpaceTask.ConnectorMapping;
import com.here.xyz.hub.task.SpaceTask.MatrixReadQuery;
import com.here.xyz.models.hub.Space.Copyright;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SpaceApi extends SpaceBasedApi {

  public SpaceApi(RouterBuilder rb) {
    rb.getRoute("getSpace").setDoValidation(false).addHandler(this::getSpace);
    rb.getRoute("getSpaces").setDoValidation(false).addHandler(this::getSpaces);
    rb.getRoute("postSpace").setDoValidation(false).addHandler(this::postSpace);
    rb.getRoute("patchSpace").setDoValidation(false).addHandler(this::patchSpace);
    rb.getRoute("deleteSpace").setDoValidation(false).addHandler(this::deleteSpace);
  }

  /**
   * Read a space.
   */
  public void getSpace(final RoutingContext context) {
    new MatrixReadQuery(
        context,
        context.pathParam(ApiParam.Path.SPACE_ID)
    ).execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * List all spaces accessible for the provided credentials.
   */
  public void getSpaces(final RoutingContext context) {
    new MatrixReadQuery(
        context,
        ApiResponseType.SPACE_LIST,
        ApiParam.Query.getBoolean(context, ApiParam.Query.INCLUDE_RIGHTS, false),
        ApiParam.Query.getBoolean(context, ApiParam.Query.INCLUDE_CONNECTORS, false),
        ApiParam.Query.getString(context, ApiParam.Query.OWNER, MatrixReadQuery.ME),
        ApiParam.Query.getSpacePropertiesQuery(context, ApiParam.Query.CONTENT_UPDATED_AT),
        ApiParam.Query.getString(context, ApiParam.Query.TAG, null),
        ApiParam.Query.getString(context, Query.REGION, null)
    ).execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Create a new space.
   */
  public void postSpace(final RoutingContext context) {
    JsonObject input;
    try {
      input = context.body().asJsonObject();
    } catch (DecodeException e) {
      context.fail(new HttpException(BAD_REQUEST, "Invalid JSON string"));
      return;
    }

    ConnectorMapping connectorMapping = ConnectorMapping.of(ApiParam.Query.getString(context, Query.CONNECTOR_MAPPING, ConnectorMapping.RANDOM.name()), ConnectorMapping.RANDOM);
    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input.getMap()), IfNotExists.CREATE, IfExists.ERROR, true, connectorMapping);

    new ConditionalOperation(context, ApiResponseType.SPACE, modifyOp, false)
        .execute(this::sendResponse, this::sendErrorResponse);
  }

  /**
   * Update a space.
   */
  public void patchSpace(final RoutingContext context) {
    JsonObject input;
    try {
      input = context.body().asJsonObject();
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
          new HttpException(BAD_REQUEST, "The resource ID in the body does not match the resource ID in the path."));
      return;
    }

    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input.getMap()), IfNotExists.ERROR, IfExists.PATCH, true);

    new ConditionalOperation(context, ApiResponseType.SPACE, modifyOp, true)
        .execute(this::sendResponse, this::sendErrorResponse);

  }

  /**
   * Delete a space.
   */
  public void deleteSpace(final RoutingContext context) {
    Map<String,Object> input = new JsonObject().put("id", context.pathParam(Path.SPACE_ID)).getMap();
    ModifySpaceOp modifyOp = new ModifySpaceOp(Collections.singletonList(input), IfNotExists.ERROR, IfExists.DELETE, true);

    //Delete the space
    ApiResponseType responseType = APPLICATION_JSON.equals(context.request().getHeader(ACCEPT))
        ? ApiResponseType.SPACE : ApiResponseType.EMPTY;
    new ConditionalOperation(context, responseType, modifyOp, true)
        .execute(this::sendResponse, this::sendErrorResponse);
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
