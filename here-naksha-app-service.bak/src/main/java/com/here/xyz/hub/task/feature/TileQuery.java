/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class TileQuery extends ReadQuery<GetFeaturesByTileEvent> {

  /**
   * A local copy of some transformation-relevant properties from the event object. NOTE: The event object is not in memory in the
   * response-phase anymore.
   *
   * @see NakshaTask#consumeEvent()
   */
  final @NotNull TransformationContext transformationContext;

  public TileQuery(
      @NotNull GetFeaturesByTileEvent event,
      @NotNull RoutingContext context,
      ApiResponseType apiResponseTypeType) {
    super(event, context, apiResponseTypeType);
    transformationContext =
        new TransformationContext(event.getX(), event.getY(), event.getLevel(), event.getMargin());
  }

  @Nonnull
  @Nonnull
  @Override
  public TaskPipeline<com.here.xyz.hub.task.feature.TileQuery> initPipeline() {
    return TaskPipeline.create(this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::transformResponse)
        .then(FeatureTaskHandler::writeCache);
  }

  static class TransformationContext {

    TransformationContext(int x, int y, int level, int margin) {
      this.x = x;
      this.y = y;
      this.level = level;
      this.margin = margin;
    }

    int x;
    int y;
    int level;
    int margin;
  }
}
