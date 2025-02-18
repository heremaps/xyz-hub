/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.hub.auth.XyzHubActionMatrix.DELETE_FEATURES;
import static com.here.xyz.hub.auth.XyzHubActionMatrix.UPDATE_FEATURES;
import static com.here.xyz.util.service.BaseHttpServerVerticle.getJWT;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.hub.connectors.models.Space;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.FeatureTask.ConditionalOperation;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import com.here.xyz.models.hub.jwt.ActionMatrix;
import com.here.xyz.models.hub.jwt.AttributeMap;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.logging.LogUtil;
import io.vertx.ext.web.RoutingContext;

public class FeatureAuthorization extends Authorization {

  @SuppressWarnings("unchecked")
  public static <X extends FeatureTask> void authorize(X task, Callback<X> callback) {
    if (task instanceof ConditionalOperation) {
      authorizeConditionalOp((ConditionalOperation) task, (Callback<ConditionalOperation>) callback);
    } else {
      authorizeReadQuery(task, callback);
    }
  }

  public static void authorizeWrite(RoutingContext context, Space space, boolean isDelete) throws HttpException {
    if(getJWT(context).skipAuth)
      return;

    final ActionMatrix tokenRights = getXyzHubMatrix(getJWT(context));
    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();

    //CREATE & UPDATE == WRITE
    if (!isDelete && !requestRights.containsKey(UPDATE_FEATURES))
      requestRights.updateFeatures(XyzHubAttributeMap.forValues(space.getOwner(), space.getId(), space.getPackages()));
    //DELETE
    else if (isDelete && !requestRights.containsKey(DELETE_FEATURES))
      requestRights.deleteFeatures(XyzHubAttributeMap.forValues(space.getOwner(), space.getId(), space.getPackages()));

    evaluateRights(LogUtil.getMarker(context), requestRights, tokenRights);
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

    // Skip authorization if the space is shared or the service is running without authorization.
    if (task.space.isShared() || task.getJwt().skipAuth) {
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


    evaluateRights(requestRights, getXyzHubMatrix(task.getJwt()), task, callback);
  }

  /**
   * Authorizes a conditional operation.
   */
  private static void authorizeConditionalOp(ConditionalOperation task, Callback<ConditionalOperation> callback) {
    if (task.getJwt().skipAuth) {
      callback.call(task);
      return;
    }

    final ActionMatrix tokenRights = getXyzHubMatrix(getJWT(task.context));
    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();

    //READ
    if (!requestRights.containsKey(XyzHubActionMatrix.READ_FEATURES) && task.modifyOp.isRead())
      requestRights.readFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    //CREATE
    else if (!requestRights.containsKey(XyzHubActionMatrix.CREATE_FEATURES) && task.modifyOp.isCreate())
      requestRights.createFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    //UPDATE
    else if (!requestRights.containsKey(UPDATE_FEATURES) && task.modifyOp.isUpdate())
      requestRights.updateFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    //DELETE
    else if (!requestRights.containsKey(DELETE_FEATURES) && task.modifyOp.isDelete())
      requestRights.deleteFeatures(XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));

    evaluateRights(requestRights, tokenRights, task, callback);
  }
}
