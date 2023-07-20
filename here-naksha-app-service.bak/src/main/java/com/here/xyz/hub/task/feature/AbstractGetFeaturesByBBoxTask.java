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

import com.here.naksha.lib.core.exceptions.ParameterError;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractGetFeaturesByBBoxTask<EVENT extends GetFeaturesByBBoxEvent>
    extends AbstractSpatialQueryTask<EVENT> {

  protected AbstractGetFeaturesByBBoxTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  protected void initEventFromRoutingContext(
      @NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
    assert queryParameters != null;
    final BBox bBox = queryParameters.getBBox();
    if (bBox == null) {
      throw new ParameterError("Missing bounding box parameters (north, east, south and west)");
    }
    event.setBbox(bBox);
    event.setClustering(queryParameters.getClustering());
    event.setTweaks(queryParameters.getTweaks());
  }
}
