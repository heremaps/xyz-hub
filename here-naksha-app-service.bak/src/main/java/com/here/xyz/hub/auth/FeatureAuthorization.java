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

import static com.here.xyz.hub.rest.Context.logId;
import static com.here.xyz.hub.rest.Context.logStream;
import static com.here.xyz.hub.rest.Context.logTime;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.here.xyz.events.Event;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.hub.rest.Context;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.ICallback;
import com.here.xyz.hub.task.feature.AbstractFeatureTask;
import com.here.xyz.hub.task.feature.ConditionalModifyFeaturesTask;
import com.here.xyz.hub.task.feature.DeleteFeaturesByTagTask;
import com.here.xyz.hub.task.feature.GetFeaturesByGeometryTask;
import io.vertx.core.json.Json;
import org.jetbrains.annotations.NotNull;

public class FeatureAuthorization extends Authorization {

  public static <E extends Event, T extends AbstractFeatureTask<E, T>> void authorize(
      @NotNull T task, @NotNull ICallback callback) {
    if (task instanceof ConditionalModifyFeaturesTask) {
      authorizeConditionalOp((ConditionalModifyFeaturesTask) task, callback);
    } else if (task instanceof DeleteFeaturesByTagTask) {
      authorizeDeleteOperation((DeleteFeaturesByTagTask) task, callback);
    } else {
      authorizeReadQuery(task, callback);
    }
  }

  /**
   * Authorizes a query operation.
   */
  private static <E extends Event, T extends AbstractFeatureTask<E, T>> void authorizeReadQuery(
      @NotNull T task, @NotNull ICallback callback) {
    final JWTPayload jwt = task.getJwt();
    if (jwt == null) {
      callback.throwException(
          new HttpException(UNAUTHORIZED, "Accessing features isn't possible without a JWT token."));
      return;
    }
    if (jwt.anonymous) {
      callback.throwException(
          new HttpException(FORBIDDEN, "Accessing features isn't possible with an anonymous token."));
      return;
    }

    if (task.space.isShared()) {
      // User is trying to read a shared space this is allowed for any authenticated user.
      callback.success();
      return;
    }

    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();
    requestRights.readFeatures(
        XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));

    final E event = task.getEvent();
    if (event instanceof GetFeaturesByBBoxEvent) {
      final String clusteringType = ((GetFeaturesByBBoxEvent) event).getClusteringType();
      if (clusteringType != null) {
        requestRights.useCapabilities(new AttributeMap().withValue("id", clusteringType + "Clustering"));
      }
    } else if (task instanceof GetFeaturesByGeometryTask) {
      final GetFeaturesByGeometryTask geometryQuery = (GetFeaturesByGeometryTask) task;
      final XyzHubAttributeMap xyzHubAttributeMap = XyzHubAttributeMap.forValues(
          geometryQuery.refSpace.getOwner(), geometryQuery.refSpaceId, geometryQuery.refSpace.getPackages());
      requestRights.readFeatures(xyzHubAttributeMap);
    }
    evaluateRights(requestRights, jwt.getXyzHubMatrix(), task, callback);
  }

  /**
   * Authorizes a conditional operation.
   */
  public static void authorizeConditionalOp(
      @NotNull ConditionalModifyFeaturesTask task, @NotNull ICallback callback) {
    final JWTPayload jwt = Context.jwt(task.routingContext);
    if (jwt == null) {
      callback.throwException(new HttpException(UNAUTHORIZED, "Missing JWT token"));
      return;
    }
    final ActionMatrix tokenRights = jwt.getXyzHubMatrix();
    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();

    // READ
    if (!requestRights.containsKey(XyzHubActionMatrix.READ_FEATURES) && task.modifyOp.isRead()) {
      requestRights.readFeatures(
          XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    }
    // CREATE
    else if (!requestRights.containsKey(XyzHubActionMatrix.CREATE_FEATURES) && task.modifyOp.isCreate()) {
      requestRights.createFeatures(
          XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    }
    // UPDATE
    else if (!requestRights.containsKey(XyzHubActionMatrix.UPDATE_FEATURES) && task.modifyOp.isUpdate()) {
      requestRights.updateFeatures(
          XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    }
    // DELETE
    else if (!requestRights.containsKey(XyzHubActionMatrix.DELETE_FEATURES) && task.modifyOp.isDelete()) {
      requestRights.deleteFeatures(
          XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    }
    evaluateRights(requestRights, tokenRights, task, callback);
  }

  /**
   * Authorizes a delete operation.
   */
  public static void authorizeDeleteOperation(@NotNull DeleteFeaturesByTagTask task, @NotNull ICallback callback) {
    final JWTPayload jwt = Context.jwt(task.routingContext);
    if (jwt == null) {
      callback.throwException(new HttpException(UNAUTHORIZED, "Missing JWT token"));
      return;
    }
    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();
    requestRights.deleteFeatures(
        XyzHubAttributeMap.forValues(task.space.getOwner(), task.space.getId(), task.space.getPackages()));
    evaluateRights(requestRights, jwt.getXyzHubMatrix(), task, callback);
  }

  protected void evaluateRights(@NotNull ActionMatrix requestRights, @NotNull ActionMatrix tokenRights) {
    final String id = logId(context);
    final String streamId = logStream(context);
    final long time = logTime(context);
    if (!tokenRights.matches(requestRights)) {
      logger.warn("{}:{}:{}us - Token access rights: {}", id, streamId, time, Json.encode(tokenRights));
      logger.warn("{}:{}:{}us - Request access rights: {}", id, streamId, time, Json.encode(requestRights));
      throw new HttpException(FORBIDDEN, getForbiddenMessage(requestRights, tokenRights));
    } else {
      logger.info("{}:{}:{}us - Token access rights: {}", id, streamId, time, Json.encode(tokenRights));
      logger.info("{}:{}:{}us - Request access rights: {}", id, streamId, time, Json.encode(requestRights));
    }
  }
}
