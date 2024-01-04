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

import io.vertx.core.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Authorization {

  private static final Logger log = LoggerFactory.getLogger(Authorization.class);

  public enum AuthorizationType {
    JWT,
    DUMMY
  }

  //  protected static void evaluateRights(
  //      @NotNull RoutingContext context,
  //      @NotNull ActionMatrix requestRights,
  //      @NotNull ActionMatrix tokenRights
  //  ) {
  //    final String id = logId(context);
  //    final String streamId = logStream(context);
  //    final long time = logTime(context);
  //    if (!tokenRights.matches(requestRights)) {
  //      log.warn("{}:{}:{}us - Token access rights: {}", id, streamId, time, Json.encode(tokenRights));
  //      log.warn("{}:{}:{}us - Request access rights: {}", id, streamId, time, Json.encode(requestRights));
  //      throw new HttpException(FORBIDDEN, getForbiddenMessage(requestRights, tokenRights));
  //    } else {
  //      log.info("{}:{}:{}us - Token access rights: {}", id, streamId, time, Json.encode(tokenRights));
  //      log.info("{}:{}:{}us - Request access rights: {}", id, streamId, time, Json.encode(requestRights));
  //    }
  //  }
  //
  //  @SuppressWarnings({"unchecked", "rawtypes"})
  //  static void evaluateRights(
  //      @NotNull ActionMatrix requestRights,
  //      @NotNull ActionMatrix tokenRights,
  //      @NotNull NakshaTask task,
  //      @NotNull ICallback callback) {
  //    try {
  //      evaluateRights(task.routingContext, requestRights, tokenRights);
  //      callback.success(task);
  //    } catch (HttpException e) {
  //      callback.throwException(e);
  //    }
  //  }

  static String getForbiddenMessage(ActionMatrix requestRights, ActionMatrix tokenRights) {
    return "Insufficient rights. Token access: " + Json.encode(tokenRights) + "\nRequest access: "
        + Json.encode(requestRights);
  }
}
