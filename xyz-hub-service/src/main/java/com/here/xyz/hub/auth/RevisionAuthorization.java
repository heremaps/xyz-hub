/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.HttpException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.web.RoutingContext;

public class RevisionAuthorization extends Authorization {

  public static Future<Void> authorize(RoutingContext context) {
    String spaceId;
    if (context == null || (spaceId = context.pathParam(Path.SPACE_ID)) == null)
      return Future.failedFuture(new HttpException(HttpResponseStatus.BAD_REQUEST, "Path parameter space id is required"));

    final Promise<Void> result = Promise.promise();
    JWTPayload jwt = Api.Context.getJWT(context);

    final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();
    requestRights.adminSpaces(XyzHubAttributeMap.forIdValues(spaceId));

    try {
      evaluateRights(Api.Context.getMarker(context), requestRights, jwt.getXyzHubMatrix());
      result.complete();
    } catch (HttpException e) {
      result.fail(e);
    }

    return result.future();
  }
}
