/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.app.service.http.auth;

import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.hub.NakshaHubConfig;
import com.here.naksha.lib.hub.NakshaHubConfig.AuthorizationMode;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.impl.JWTAuthHandlerImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NakshaJwtAuthHandler extends JWTAuthHandlerImpl {

  protected static final Logger logger = LoggerFactory.getLogger(NakshaJwtAuthHandler.class);

  private final NakshaHubConfig hubConfig;
  private static final String MASTER_JWT_RESOURCE_FILE = "auth/dummyMasterJwt.json";
  private static final JsonObject MASTER_JWT_PAYLOAD = new JsonObject(IoHelp.readResource(MASTER_JWT_RESOURCE_FILE));
  /**
   * The master JWT used for testing.
   */
  private final String MASTER_JWT = authProvider.generateToken(MASTER_JWT_PAYLOAD);

  public NakshaJwtAuthHandler(
      @NotNull JWTAuth authProvider, @NotNull NakshaHubConfig hubConfig, @Nullable String realm) {
    super(authProvider, realm);
    this.hubConfig = hubConfig;
  }

  @Override
  public void authenticate(@NotNull RoutingContext context, @NotNull Handler<@NotNull AsyncResult<User>> handler) {
    if (hubConfig.authMode == AuthorizationMode.DUMMY
        && !context.request().headers().contains(HttpHeaders.AUTHORIZATION)) {
      // Use the master JWT for testing in DUMMY auth mode with no JWT provided in request
      context.request().headers().set(HttpHeaders.AUTHORIZATION, "Bearer " + MASTER_JWT);
    }
    // TODO: If compressed JWTs are supported
    //    if (ALLOW_COMPRESSED_JWT && jwt != null && !isJWT(jwt)) {
    //      try {
    //        byte[] bytearray = Base64.getDecoder().decode(jwt.getBytes());
    //        bytearray = Compression.decompressUsingInflate(bytearray);
    //        jwt = new String(bytearray);
    //      } catch (Exception e) {
    //        logger.error(Context.getMarker(context), "JWT Base64 decoding or decompression failed: " + jwt, e);
    //        handler.handle(Future.failedFuture("Wrong auth credentials format."));
    //        return;
    //      }
    //    }
    super.authenticate(context, handler);
  }
}
