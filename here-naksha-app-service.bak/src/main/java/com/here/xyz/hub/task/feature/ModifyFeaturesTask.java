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
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection;
import com.here.naksha.lib.core.models.hub.Space;
import com.here.xyz.Typed;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.responses.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Minimal version of Conditional Operation used on admin events endpoint. Contains some limitations because doesn't implement the full
 * pipeline. If you want to use the full conditional operation pipeline, you should request through Features API. Known limitations: -
 * Cannot perform validation of existing resources per operation type
 */
public class ModifyFeaturesTask extends AbstractFeatureTask<ModifyFeaturesEvent> {

  public ModifyFeaturesTask() {
    super(new ModifyFeaturesEvent());
  }

  @Override
  protected void initEventFromRoutingContext(
      @NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
    assert queryParameters != null;

    final @Nullable String prefixId = queryParameters.getPrefixId();
    final @Nullable Typed body = setBody(null);
    if (body instanceof FeatureCollection collection) {
      final List<@NotNull Feature> features = collection.getFeatures();
    } else if (body instanceof Feature feature) {

    } else if (body == null) {
      throw new ParameterError("Missing body");
    } else {
      throw new ParameterError("Invalid body, expected a feature collection");
    }
    requestMatrix.createFeatures(event.getSpace());
  }

  @Override
  protected @NotNull XyzResponse execute() throws Exception {
    final Space space = event.getSpace();
    pipeline.addSpaceHandler(space);
    return sendAuthorizedEvent(event, requestMatrix());
  }
}
