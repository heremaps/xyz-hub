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
import com.here.naksha.lib.core.util.diff.ConflictResolution;
import com.here.naksha.lib.core.util.modify.IfExists;
import com.here.naksha.lib.core.util.modify.IfNotExists;
import com.here.naksha.lib.core.util.modify.XyzModificationList;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.responses.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ConditionalModifyFeaturesTask extends AbstractFeatureTask<ModifyFeaturesEvent> {

  public @Nullable XyzModificationList modifyOp;
  public @Nullable IfNotExists ifNotExists;
  public @Nullable IfExists ifExists;
  public boolean transactional;
  public @Nullable ConflictResolution conflictResolution;
  public List<Feature> unmodifiedFeatures;
  public final boolean requireResourceExists;
  public List<String> addTags;
  public List<String> removeTags;
  public String prefixId;
  public Map<Object, Integer> positionById;
  public LoadFeaturesEvent loadFeaturesEvent;
  public boolean hasNonModified;

  public ConditionalModifyFeaturesTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public @NotNull ModifyFeaturesEvent createEvent() {
    return new ModifyFeaturesEvent();
  }

  @Override
  public void initEventFromRoutingContext(
      @NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws ParameterError {
    super.initEventFromRoutingContext(routingContext, responseType);
  }

  @Override
  protected @NotNull XyzResponse execute() throws Exception {
    // TODO: Send LoadFeaturesEvent
    //
    pipeline.addSpaceHandler(getSpace());
    return sendAuthorizedEvent(event, requestMatrix());
  }

  /*


  public ConditionalModifyFeaturesTask(
  @NotNull ModifyFeaturesEvent event,
  @NotNull RoutingContext context,
  ApiResponseType apiResponseTypeType,
  @Nullable ModifyOp.IfNotExists ifNotExists,
  @Nullable ModifyOp.IfExists ifExists,
  boolean transactional,
  @Nullable Patcher.ConflictResolution conflictResolution,
  boolean requireResourceExists
  ) {
  this(event, context, apiResponseTypeType, null, requireResourceExists);
  this.ifNotExists = ifNotExists;
  this.ifExists = ifExists;
  this.transactional = transactional;
  this.conflictResolution = conflictResolution;
  }


  @Override
  public void initPipeline(@NotNull TaskPipeline<ModifyFeaturesEvent, ConditionalModifyFeaturesTask> pipeline) {
  pipeline
  .then(FeatureTaskHandler::resolveSpace)
  .then(FeatureTaskHandler::registerRequestMemory)
  .then(FeatureTaskHandler::throttle)
  .then(FeatureTaskHandler::injectSpaceParams)
  .then(FeatureTaskHandler::checkPreconditions)
  .then(FeatureTaskHandler::prepareModifyFeatureOp)
  .then(FeatureTaskHandler::preprocessConditionalOp)
  .then(FeatureTaskHandler::loadObjects)
  .then(FeatureTaskHandler::verifyResourceExists)
  .then(FeatureTaskHandler::updateTags)
  .then(FeatureTaskHandler::monitorFeatureRequest)
  .then(FeatureTaskHandler::processConditionalOp)
  .then(FeatureAuthorization::authorize)
  .then(FeatureTaskHandler::enforceUsageQuotas)
  .then(FeatureTaskHandler::extractUnmodifiedFeatures)
  .then(FeatureTaskHandler::invoke);
  }

  private ConditionalModifyFeaturesTask buildConditionalOperation(
  ModifyFeaturesEvent event,
  RoutingContext context,
  ApiResponseType apiResponseTypeType,
  List<Map<String, Object>> featureModifications,
  IfNotExists ifNotExists,
  IfExists ifExists,
  boolean transactional,
  ConflictResolution cr,
  boolean requireResourceExists,
  int bodySize) {
  if (featureModifications == null)
  return new ConditionalModifyFeaturesTask(event, context, apiResponseTypeType, ifNotExists, ifExists, transactional, cr, requireResourceExists, bodySize);

  final ModifyFeatureOp modifyFeatureOp = new ModifyFeatureOp(featureModifications, ifNotExists, ifExists, transactional, cr);
  return new ConditionalModifyFeaturesTask(event, context, apiResponseTypeType, modifyFeatureOp, requireResourceExists, bodySize);
  }

   */
}
