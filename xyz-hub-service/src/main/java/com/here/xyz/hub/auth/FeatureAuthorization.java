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

package com.here.xyz.hub.auth;

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.spi.AuthorizationHandler;
import com.here.xyz.hub.spi.Modules;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.FeatureTask.ConditionalOperation;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.TaskPipeline.Callback;

public class FeatureAuthorization extends Authorization {
  static final AuthorizationHandler authorizationHandler = Modules.getAuthorizationHandler();

  @SuppressWarnings("unchecked")
  public static <X extends FeatureTask> void authorize(X task, Callback<X> callback) {
    if (task instanceof ConditionalOperation) {
      authorizeConditionalOp((ConditionalOperation) task, (Callback<ConditionalOperation>) callback);
    } else {
      authorizeReadQuery(task, callback);
    }

    if (!authorizationHandler.authorize(task.context)) {
      callback.exception(new HttpException(UNAUTHORIZED, "Unauthorized access: " + task.context.request().method() + " " + task.context.request().path()));
    }
  }

  /**
   * Authorizes a query operation.
   */
  private static <X extends FeatureTask> void authorizeReadQuery(X task, Callback<X> callback) {
    //Check if anonymous token is being used
    if (task.getJwt().anonymous) {
      callback.exception(new HttpException(FORBIDDEN, "Accessing features isn't possible with an anonymous token."));
      return;
    }

    if (task.space.isShared()) {
      //User is trying to read a shared space this is allowed for any authenticated user
      callback.call(task);
      return;
    }

    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();
    requestRights.readFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));

    if (task.getEvent() instanceof GetFeaturesByBBoxEvent) {
      String clusteringType = ((GetFeaturesByBBoxEvent) task.getEvent()).getClusteringType();
      if (clusteringType != null) {
        requestRights.useCapabilities(new AttributeMap().withValue("id", clusteringType + "Clustering"));
      }
    }
    else if(task.getEvent() instanceof GetFeaturesByGeometryEvent) {
      if(((GeometryQuery)task).refSpaceId != null)
        requestRights.readFeatures(XyzHubAttributeMap.forValues( ((GeometryQuery)task).refSpace.getOwner(), ((GeometryQuery)task).refSpaceId , ((GeometryQuery)task).refSpace.getPackages()));
    }

    evaluateRights(requestRights, task.getJwt().getXyzHubMatrix(), task, callback);
  }

  /**
   * Authorizes a conditional operation.
   */
  private static void authorizeConditionalOp(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    JWTPayload jwt = Api.Context.getJWT(task.context);

    final ActionMatrix tokenRights = jwt.getXyzHubMatrix();
    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();

    //READ
    if (!requestRights.containsKey(XyzHubActionMatrix.READ_FEATURES) && task.modifyOp.isRead())
      requestRights.readFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    //CREATE
    else if (!requestRights.containsKey(XyzHubActionMatrix.CREATE_FEATURES) && task.modifyOp.isCreate())
      requestRights.createFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    //UPDATE
    else if (!requestRights.containsKey(XyzHubActionMatrix.UPDATE_FEATURES) && task.modifyOp.isUpdate())
      requestRights.updateFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    //DELETE
    else if (!requestRights.containsKey(XyzHubActionMatrix.DELETE_FEATURES) && task.modifyOp.isDelete())
      requestRights.deleteFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));

    evaluateRights(requestRights, tokenRights, task, callback);
  }
}
