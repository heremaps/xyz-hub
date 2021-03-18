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

package com.here.xyz.hub.auth;

import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.here.xyz.hub.rest.ApiParam.Query;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.impl.AuthenticationHandlerImpl;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import java.util.List;

public class JWTURIHandler extends AuthenticationHandlerImpl<JWTAuth> implements JWTAuthHandler {

  private JWTURIHandler(JWTAuth authProvider) {
    super(authProvider);
  }

  public static JWTAuthHandler create(JWTAuth authProvider) {
    return new JWTURIHandler(authProvider);
  }

  @Override
  public void parseCredentials(RoutingContext context, Handler<AsyncResult<Credentials>> handler) {
    final List<String> access_token = Query.queryParam(Query.ACCESS_TOKEN, context);
    if (access_token != null && access_token.size() > 0) {
      handler.handle(Future.succeededFuture(new TokenCredentials(access_token.get(0))));
      return;
    }
    handler.handle(Future.failedFuture(new HttpStatusException(UNAUTHORIZED.code(), "Missing auth credentials.")));
  }

  @Override
  public String authenticateHeader(RoutingContext context) {
    return "Bearer";
  }
}
