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

import static com.here.xyz.hub.rest.Context.logId;
import static com.here.xyz.hub.rest.Context.logStream;
import static com.here.xyz.hub.rest.Context.logTime;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.AbstractEventTask;
import com.here.xyz.hub.task.ICallback;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

public abstract class Authorization {

  private static final Logger logger = LogManager.getLogger();

  public enum AuthorizationType {
    JWT,
    DUMMY
  }

  protected static void evaluateRights(
      @NotNull RoutingContext context,
      @NotNull ActionMatrix requestRights,
      @NotNull ActionMatrix tokenRights
  ) throws HttpException {
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  static void evaluateRights(
      @NotNull ActionMatrix requestRights,
      @NotNull ActionMatrix tokenRights,
      @NotNull AbstractEventTask task,
      @NotNull ICallback callback) {
    try {
      evaluateRights(task.routingContext, requestRights, tokenRights);
      callback.success(task);
    } catch (HttpException e) {
      callback.throwException(e);
    }
  }

  static String getForbiddenMessage(ActionMatrix requestRights, ActionMatrix tokenRights) {
    return "Insufficient rights. Token access: " + Json.encode(tokenRights) + "\nRequest access: " + Json.encode(requestRights);
  }
}
