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

import com.here.naksha.lib.core.util.fib.FibMapEntry;
import com.here.naksha.lib.core.util.fib.FibSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.auth.jwt.impl.JWTAuthProviderImpl;
import org.jetbrains.annotations.NotNull;

public final class NakshaAuthProvider extends JWTAuthProviderImpl {

  public NakshaAuthProvider(@NotNull Vertx vertx, JWTAuthOptions config) {
    super(vertx, config);
  }

  private static final FibSet<String, FibMapEntry<String, User>> userCache = new FibSet<>(FibMapEntry::new);

  @Override
  public void authenticate(
      @NotNull Credentials credentials, @NotNull Handler<@NotNull AsyncResult<User>> resultHandler) {
    TokenCredentials authInfo = (TokenCredentials) credentials;
    final String jwt = authInfo.getToken();

    final FibMapEntry<String, User> entry = userCache.put(jwt, FibSet.WEAK);
    if (entry.getValue() != null) {
      resultHandler.handle(Future.succeededFuture(entry.getValue()));
      return;
    }

    super.authenticate(authInfo, (authResult) -> {
      if (authResult.failed()) {
        resultHandler.handle(Future.failedFuture(authResult.cause()));
        return;
      }

      final User user = authResult.result();
      assert user != null;
      entry.setValue(user);
      resultHandler.handle(Future.succeededFuture(user));
    });
  }
}
