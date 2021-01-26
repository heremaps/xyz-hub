/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

import com.here.xyz.hub.util.Compression;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.auth.jwt.impl.JWTAuthProviderImpl;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.StringUtils;

public class XyzAuthProvider extends JWTAuthProviderImpl {

  public XyzAuthProvider(Vertx vertx, JWTAuthOptions config) {
    super(vertx, config);
  }

  protected final ExpiringMap<String, User> usersCache = ExpiringMap.builder()
      .maxSize(8 * 1024)
      .expirationPolicy(ExpirationPolicy.CREATED)
      .expiration(10, TimeUnit.MINUTES)
      .build();

  @Override
  public void authenticate(Credentials credentials, Handler<AsyncResult<User>> resultHandler) {
    TokenCredentials authInfo = (TokenCredentials) credentials;
    final String jwt = authInfo.getToken();

    if (!isJWT(jwt)) {
      try {
        byte[] bytearray = Base64.getDecoder().decode(jwt.getBytes());
        bytearray = Compression.decompressUsingInflate(bytearray);
        authInfo.setToken(new String(bytearray));
      }
      catch (DataFormatException e) {
        resultHandler.handle(Future.failedFuture("Wrong auth credentials format."));
        return;
      }
    }

    User cachedUser = usersCache.get(jwt);
    if (cachedUser != null) {
      resultHandler.handle(Future.succeededFuture(cachedUser));
      return;
    }

    super.authenticate(authInfo, (authResult) -> {
      if (authResult.failed()) {
        resultHandler.handle(Future.failedFuture(authResult.cause()));
        return;
      }

      final User user = authResult.result();
      user.principal().put("jwt", jwt);

      usersCache.put(jwt, user);
      resultHandler.handle(Future.succeededFuture(user));
    });
  }

  private boolean isJWT(final String jwt) {
    return StringUtils.countMatches(jwt, ".") == 2;
  }
}
