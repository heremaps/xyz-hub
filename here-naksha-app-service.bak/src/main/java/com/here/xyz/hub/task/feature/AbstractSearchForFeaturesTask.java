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
import com.here.xyz.events.feature.SearchForFeaturesEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractSearchForFeaturesTask<EVENT extends SearchForFeaturesEvent>
    extends AbstractFeatureTask<EVENT> {

  protected AbstractSearchForFeaturesTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  protected void initEventFromRoutingContext(
      @NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);

    assert queryParameters != null;
    event.setLimit(queryParameters.getLimit());
    event.setTags(queryParameters.getTagsQuery());
    event.setPropertiesQuery(queryParameters.getPropertiesQuery());
    event.setForce2D(queryParameters.getForce2D());
    event.setSelection(queryParameters.getSelection());
  }
}
