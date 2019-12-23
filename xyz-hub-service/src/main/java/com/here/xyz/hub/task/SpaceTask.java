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

package com.here.xyz.hub.task;


import static com.here.xyz.hub.task.ModifyOp.IfExists.DELETE;
import static com.here.xyz.hub.task.ModifyOp.IfExists.MERGE;
import static com.here.xyz.hub.task.ModifyOp.IfExists.PATCH;
import static com.here.xyz.hub.task.ModifyOp.IfExists.REPLACE;
import static com.here.xyz.hub.task.ModifyOp.IfExists.RETAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.google.common.base.Strings;
import com.here.xyz.events.Event;
import com.here.xyz.hub.auth.SpaceAuthorization;
import com.here.xyz.hub.config.SpaceConfigClient.SpaceAuthorizationCondition;
import com.here.xyz.hub.config.SpaceConfigClient.SpaceSelectionCondition;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ModifyOp.IfExists;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import io.vertx.ext.web.RoutingContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class SpaceTask<X extends SpaceTask<?>> extends Task<Event, X> {

  public static final String DEFAULT_STORAGE_ID = "psql";

  public View view = View.BASIC;

  /**
   * All spaces in the response.
   */
  public List<Space> responseSpaces;
  public boolean canReadAdminProperties = false;
  public boolean canReadConnectorsProperties = false;

  public SpaceTask(RoutingContext context, ApiResponseType responseType) {
    super(new SpaceEvent(), context, responseType, true);
  }

  public enum View {
    BASIC,
    BASIC_RIGHTS,
    CONNECTOR_RIGHTS,
    FULL
  }

  public abstract static class ReadQuery<X extends ReadQuery<?>> extends SpaceTask<X> {

    /**
     * The ID of the space to read.
     */
    public List<String> spaceIds;
    public List<String> ownerIds;

    public SpaceAuthorizationCondition authorizedCondition;
    public SpaceSelectionCondition selectedCondition;

    public ReadQuery(RoutingContext context, ApiResponseType returnType, List<String> spaceIds, List<String> ownerIds) {
      super(context, returnType);
      this.spaceIds = spaceIds;
      this.ownerIds = ownerIds;
    }

    public void addOwnerId(String ownerId) {
      if (ownerIds == null) {
        ownerIds = Collections.singletonList(ownerId);
      } else {
        ownerIds.add(ownerId);
      }
    }
  }

  public static class MatrixReadQuery extends ReadQuery<MatrixReadQuery> {

    //Special "ownerIds" referring also to groups of owners
    //NOTE: It's also allowed some owner's ID directly
    public static final String ME = "me";
    public static final String OTHERS = "others";
    public static final String ALL = "*";

    public MatrixReadQuery(RoutingContext context, ApiResponseType returnType, boolean includeRights, boolean includeConnectors, String owner) {
      super(context, returnType, null, null);
      if (!Strings.isNullOrEmpty(owner)) {
        selectedCondition = new SpaceSelectionCondition();
        String ownOwnerId = Api.Context.getJWT(context).aid;
        switch (owner) {
          case ME:
            selectedCondition.ownerIds = Collections.singleton(ownOwnerId);
            selectedCondition.shared = false;
            break;
          case OTHERS:
            selectedCondition.ownerIds = Collections.singleton(ownOwnerId);
            selectedCondition.negateOwnerIds = true;
            break;
          case ALL:
            //Nothing to filter in the selectedCondition
            break;
          default:
            //Assuming a specific ownerId has been defined
            selectedCondition.ownerIds = Collections.singleton(owner);
        }
      }

      if (includeRights) {
        this.view = View.BASIC_RIGHTS;
      }

      this.canReadConnectorsProperties = includeConnectors;
    }

    @Override
    public TaskPipeline getPipeline() {
      return TaskPipeline.create(this)
          .then(SpaceAuthorization::authorizeReadSpaces)
          .then(SpaceTaskHandler::readFromJWT)
          .then(SpaceTaskHandler::readSpaces)
          .then(SpaceTaskHandler::convertResponse);
    }
  }

  public static class ConditionalOperation extends SpaceTask<ConditionalOperation> {

    private static final List<IfExists> UPDATE_OPS = Arrays.asList(PATCH, MERGE, REPLACE);
    public final boolean requireResourceExists;
    public ModifySpaceOp modifyOp;
    public Space template;

    public ConditionalOperation(RoutingContext context, ApiResponseType returnType, ModifySpaceOp modifyOp, boolean requireResourceExists) {
      super(context, returnType);
      this.modifyOp = modifyOp;
      this.requireResourceExists = requireResourceExists;
    }

    private void verifyResourceExists(ConditionalOperation task, Callback<ConditionalOperation> callback) {
      if (task.requireResourceExists && task.modifyOp.entries.get(0).head == null) {
        callback.exception(new HttpException(NOT_FOUND, "The requested resource does not exist."));
      } else {
        callback.call(task);
      }
    }

    public boolean isRead() {
      try {
        return modifyOp.entries.get(0).head != null && modifyOp.ifExists.equals(RETAIN);
      } catch (NullPointerException e) {
        return false;
      }
    }

    public boolean isCreate() {
      try {
        return modifyOp.entries.get(0).head == null && modifyOp.entries.get(0).result != null;
      } catch (NullPointerException e) {
        return false;
      }
    }

    public boolean isDelete() {
      try {
        return modifyOp.entries.get(0).head != null && modifyOp.ifExists.equals(DELETE);
      } catch (NullPointerException e) {
        return false;
      }
    }

    public boolean isUpdate() {
      try {
        return modifyOp.entries.get(0).head != null && UPDATE_OPS.contains(modifyOp.ifExists);
      } catch (NullPointerException e) {
        return false;
      }
    }

    @Override
    public TaskPipeline<ConditionalOperation> getPipeline() {
      return TaskPipeline.create(this)
          .then(SpaceTaskHandler::loadSpace)
          .then(SpaceTaskHandler::preprocess)
          .then(this::verifyResourceExists)
          .then(SpaceTaskHandler::processModifyOp)
          .then(SpaceAuthorization::authorizeModifyOp)
          .then(SpaceTaskHandler::enforceUsageQuotas)
          .then(SpaceTaskHandler::validate)
          .then(SpaceTaskHandler::timestamp)
          .then(SpaceTaskHandler::sendEvents)
          .then(SpaceTaskHandler::modifySpaces)
          .then(SpaceTaskHandler::convertResponse);
    }
  }

  public static class SpaceEvent extends Event<SpaceEvent> {

  }
}
