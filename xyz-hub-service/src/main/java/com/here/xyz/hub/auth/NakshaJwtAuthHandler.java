/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import static com.here.xyz.hub.rest.ApiParam.Query.ACCESS_TOKEN;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization.AuthorizationType;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.util.Compression;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.impl.JWTAuthHandlerImpl;
import java.util.Base64;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NakshaJwtAuthHandler extends JWTAuthHandlerImpl {

  protected static final Logger logger = LoggerFactory.getLogger(NakshaJwtAuthHandler.class);

  final String RAW_TOKEN = "RAW_TOKEN";

  /**
   * Indicates, if compressed JWTs are allowed.
   */
  final boolean ALLOW_COMPRESSED_JWT = true;

  /**
   * Indicates, if the bearer token could be sent in the request URI query component as defined in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6750#section-2.3">RFC-6750 Section 2.3</a>
   */
  final boolean ALLOW_URI_QUERY_PARAMETER = true;

  /**
   * Indicates, if anonymous access is allowed.
   */
  final boolean ALLOW_ANONYMOUS_ACCESS = Service.configuration.XYZ_HUB_AUTH == AuthorizationType.DUMMY;

  private static final String ANONYMOUS_JWT_RESOURCE_FILE = "auth/dummyJwt.json";
  private static final String ANONYMOUS_JWT = JwtKey.generateToken(ANONYMOUS_JWT_RESOURCE_FILE);

  public NakshaJwtAuthHandler(@NotNull JWTAuth authProvider, @Nullable String realm) {
    super(authProvider, realm);
  }

  @Override
  public void authenticate(@NotNull RoutingContext context, @NotNull Handler<@NotNull AsyncResult<User>> handler) {
    String jwt = getFromAuthHeader(context.request().headers().get(HttpHeaders.AUTHORIZATION));

    // Try to get the token from the query parameter
    if (ALLOW_URI_QUERY_PARAMETER && jwt == null) {
      final List<String> accessTokenParam = Query.queryParam(ACCESS_TOKEN, context);
      if (accessTokenParam != null && accessTokenParam.size() > 0) {
        jwt = accessTokenParam.get(0);
        if (jwt != null) context.put(ACCESS_TOKEN, jwt);
      }
    }

    // If anonymous access is allowed, use the default anonymous JWT token
    if (ALLOW_ANONYMOUS_ACCESS && jwt == null) {
      jwt = ANONYMOUS_JWT;
    }

    // stores the token (raw, as it was received) temporarily in the context
    context.put(RAW_TOKEN, jwt);

    // If compressed JWTs are supported
    if (ALLOW_COMPRESSED_JWT && jwt != null && !isJWT(jwt)) {
      try {
        byte[] bytearray = Base64.getDecoder().decode(jwt.getBytes());
        bytearray = Compression.decompressUsingInflate(bytearray);
        jwt = new String(bytearray);
      } catch (Exception e) {
        logger.error(Context.getMarker(context), "JWT Base64 decoding or decompression failed: " + jwt, e);
        handler.handle(Future.failedFuture("Wrong auth credentials format."));
        return;
      }
    }

    if (jwt != null) {
      context.request().headers().set(HttpHeaders.AUTHORIZATION, "Bearer " + jwt);
    }

    super.authenticate(context, authn -> {
      if (authn.failed()) {
        handler.handle(Future.failedFuture(new HttpException(401, authn.cause())));
      }
      else {
        authn.result().principal().put("jwt", context.remove(RAW_TOKEN));
        handler.handle(authn);
      }
    });
  }

  private @Nullable String getFromAuthHeader(@Nullable String authHeader) {
    return (authHeader != null && authHeader.startsWith("Bearer ")) ? authHeader.substring(7) : null;
  }

  private boolean isJWT(final @Nullable String jwt) {
    return StringUtils.countMatches(jwt, ".") == 2;
  }
}
