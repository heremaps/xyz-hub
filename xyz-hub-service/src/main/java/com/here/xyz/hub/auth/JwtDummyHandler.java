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

import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization.AuthorizationType;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.impl.AuthHandlerImpl;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JwtDummyHandler extends AuthHandlerImpl implements JWTAuthHandler {

  private static final Logger logger = LogManager.getLogger();
  private final JsonObject options = new JsonObject();
  private static final String DUMMY_JWT_RESOURCE_FILE = "/auth/dummyJwt.json";
  private static volatile String dummyJwt = JwtGenerator.generateToken(DUMMY_JWT_RESOURCE_FILE);

  static {
    logger.info("DUMMY token was created.");
  }

  private JwtDummyHandler(JWTAuth authProvider) {
    super(authProvider, "Bearer");
  }

  public static JWTAuthHandler create(JWTAuth authProvider) {
    return new JwtDummyHandler(authProvider);
  }

  private static String getDummyJwt() {
    if (Service.configuration.XYZ_HUB_AUTH != AuthorizationType.DUMMY) {
      throw new IllegalStateException("DUMMY authorization is not activate. DUMMY JWT can't be used.");
    }
    return dummyJwt;
  }

  public void parseCredentials(RoutingContext context, Handler<AsyncResult<JsonObject>> handler) {
    try {
      handler.handle(Future.succeededFuture(new JsonObject().put("jwt", getDummyJwt()).put("options", options)));
    } catch (Exception e) {
      handler.handle(Future.failedFuture(new HttpException(UNAUTHORIZED, "DUMMY Authorization failed.", e)));
    }
  }

  @Override
  protected String authenticateHeader(RoutingContext context) {
    return null;
  }

  @Override
  public JWTAuthHandler setAudience(List<String> audience) {
    options.put("audience", new JsonArray(audience));
    return this;
  }

  @Override
  public JWTAuthHandler setIssuer(String issuer) {
    options.put("issuer", issuer);
    return this;
  }

  @Override
  public JWTAuthHandler setIgnoreExpiration(boolean ignoreExpiration) {
    options.put("ignoreExpiration", ignoreExpiration);
    return this;
  }
}
