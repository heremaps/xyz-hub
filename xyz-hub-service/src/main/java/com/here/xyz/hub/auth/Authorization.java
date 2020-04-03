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

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;

import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.task.Task;
import com.here.xyz.hub.task.TaskPipeline.Callback;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class Authorization {

  private static final Logger logger = LogManager.getLogger();

  public enum AuthorizationType {
    JWT,
    DUMMY
  }

  protected static void evaluateRights(Marker marker, ActionMatrix requestRights, ActionMatrix tokenRights) throws HttpException {
    if (tokenRights == null || !tokenRights.matches(requestRights)) {
      logger.warn(marker, "Token access rights: {}", Json.encode(tokenRights));
      logger.warn(marker, "Request access rights: {}", Json.encode(requestRights));
      throw new HttpException(FORBIDDEN, getForbiddenMessage(requestRights, tokenRights));
    }
    else {
      logger.info(marker, "Token access rights: {}", Json.encode(tokenRights));
      logger.info(marker, "Request access rights: {}", Json.encode(requestRights));
    }
  }

  static <X extends Task> void evaluateRights(ActionMatrix requestRights, ActionMatrix tokenRights, X task, Callback<X> callback) {
    try {
      evaluateRights(task.getMarker(), requestRights, tokenRights);
      callback.call(task);
    } catch (HttpException e) {
      callback.exception(e);
    }
  }

  static String getForbiddenMessage(ActionMatrix requestRights, ActionMatrix tokenRights) {
    return "Insufficient rights. Token access: " + Json.encode(tokenRights) + "\nRequest access: " + Json.encode(requestRights);
  }
}
