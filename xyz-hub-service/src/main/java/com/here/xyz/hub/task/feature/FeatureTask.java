/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.hub.task.feature;

import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.XyzHubTask;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.XyzError;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;

/**
 * All tasks related to features in a space need to extend this class.
 */
public abstract class FeatureTask extends XyzHubTask {

  /**
   * Create a new feature task, extract space-id from path-parameters.
   *
   * @param routingContext the routing context.
   * @param responseType   the response type requested.
   * @throws XyzErrorException If any error occurred.
   */
  FeatureTask(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws XyzErrorException {
    super(routingContext, responseType);

    if (routingContext.pathParam(ApiParam.Path.SPACE_ID) == null) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing space path parameter");
    }
    final @NotNull String spaceId = routingContext.pathParam(ApiParam.Path.SPACE_ID);
    if (spaceId == null) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Missing space path parameter");
    }
    final Space space = Space.getSpaceById(spaceId);
    if (space == null) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, "Unknown space " + spaceId);
    }
    this.space = space;
  }

  /**
   * The space for this operation.
   */
  protected final @NotNull Space space;

}