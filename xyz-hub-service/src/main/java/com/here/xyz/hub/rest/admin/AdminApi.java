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

package com.here.xyz.hub.rest.admin;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;

import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;

public class AdminApi extends Api {

  public static final String MAIN_ADMIN_ENDPOINT = "/hub/admin/";
  public static final String ADMIN_MESSAGE_ENDPOINT = MAIN_ADMIN_ENDPOINT + "messages";
  private static final MessageBroker messageBroker = MessageBroker.getInstance();

  private static final String ADMIN_CAPABILITY_MESSAGING = "messaging";

  public AdminApi(Vertx vertx, Router router, AuthHandler auth) {
    router.route(HttpMethod.POST, ADMIN_MESSAGE_ENDPOINT)
        .handler(auth)
        .handler(this::onMessage);
  }

  private void onMessage(final RoutingContext context) {
    try {
      AdminAuthorization.authorizeAdminMessaging(context);
      messageBroker.receiveRawMessage(context.getBody().getBytes());
      context.response().setStatusCode(NO_CONTENT.code())
          .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
          .end();
    }
    catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  private static class AdminAuthorization extends Authorization {
    public static void authorizeAdminMessaging(RoutingContext context) throws HttpException {
      JWTPayload jwt = Api.Context.getJWT(context);
      final ActionMatrix tokenRights = jwt.getXyzHubMatrix();
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
          .useAdminCapabilities(XyzHubAttributeMap.forIdValues(ADMIN_CAPABILITY_MESSAGING));

      evaluateRights(Api.Context.getMarker(context), requestRights, tokenRights);
    }
  }
}
