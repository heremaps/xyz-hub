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
package com.here.naksha.lib.handlers;

import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.*;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.handlers.DefaultViewHandlerProperties.ViewType;
import com.here.naksha.lib.view.*;
import com.here.naksha.lib.view.merge.MergeByStoragePriority;
import com.here.naksha.lib.view.missing.IgnoreMissingResolver;
import com.here.naksha.lib.view.missing.ObligatoryLayersResolver;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultViewHandler extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(DefaultViewHandler.class);

  private final @NotNull EventHandler eventHandler;
  private final @NotNull EventTarget<?> eventTarget;
  private final @NotNull DefaultViewHandlerProperties properties;

  public DefaultViewHandler(
      final @NotNull EventHandler eventHandler,
      final @NotNull INaksha hub,
      final @NotNull EventTarget<?> eventTarget) {
    super(hub);
    this.eventHandler = eventHandler;
    this.eventTarget = eventTarget;
    this.properties = JsonSerializable.convert(eventHandler.getProperties(), DefaultViewHandlerProperties.class);
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    final Request<?> request = event.getRequest();
    if (request instanceof ReadFeatures || request instanceof WriteFeatures) {
      return PROCESS;
    } else if (request instanceof WriteXyzCollections) {
      return SUCCEED_WITHOUT_PROCESSING;
    }
    return NOT_IMPLEMENTED;
  }

  @Override
  public @NotNull Result process(@NotNull IEvent event) {

    final NakshaContext ctx = NakshaContext.currentContext();
    final Request<?> request = event.getRequest();
    logger.info("Handler received request {}", request.getClass().getSimpleName());

    final String storageId = properties.getStorageId();

    if (storageId == null) {
      logger.error("No storageId configured");
      return new ErrorResult(XyzError.NOT_FOUND, "No storageId configured for handler.");
    }
    logger.info("Against Storage id={}", storageId);
    addStorageIdToStreamInfo(storageId, ctx);

    final IStorage storageImpl = nakshaHub().getStorageById(storageId);
    logger.info("Using storage implementation [{}]", storageImpl.getClass().getName());

    if (storageImpl instanceof IView view) {

      if (properties.getSpaceIds() == null || properties.getSpaceIds().isEmpty()) {
        logger.error("No spaces configured, so can't process this request");
        return new ErrorResult(XyzError.NOT_FOUND, "No spaces configured for handler.");
      } else {

        view.setViewLayerCollection(
            prepareViewLayerCollection(nakshaHub().getSpaceStorage(), properties.getSpaceIds()));
        // TODO MCPODS-7046 Replace the way how view is created. Should be immutable without need to use set
        // method.
        return processRequest(ctx, view, request);
      }
    } else {
      logger.error("Associated storage doesn't implement View, so can't process this request");
      return new ErrorResult(XyzError.EXCEPTION, "Associated storage doesn't implement View");
    }
  }

  private Result processRequest(NakshaContext ctx, IView view, Request<?> request) {
    if (request instanceof ReadFeatures rf) {
      return forwardReadFeatures(ctx, view, rf);
    } else if (request instanceof WriteFeatures<?, ?, ?> wf) {
      return forwardWriteFeatures(ctx, view, wf);
    } else if (request instanceof WriteCollections<?, ?, ?> wc) {
      return forwardWriteFeatures(ctx, view, wc);
    } else {
      return notImplemented(request);
    }
  }

  private Result forwardWriteFeatures(NakshaContext ctx, IView view, WriteRequest<?, ?, ?> wr) {
    try (final IWriteSession writeSession = view.newWriteSession(ctx, true)) {
      return writeSession.execute(wr);
    }
  }

  private Result forwardReadFeatures(NakshaContext ctx, IView view, ReadFeatures rf) {

    try (final ViewReadSession reader = (ViewReadSession) view.newReadSession(ctx, false)) {
      final MissingIdResolver<XyzFeature, XyzFeatureCodec> resolver;
      if (properties.getViewType() == ViewType.UNION) {
        resolver = new IgnoreMissingResolver<>();
      } else {
        final Set<ViewLayer> obligatoryLayers = getObligatoryLayers(view.getViewCollection());
        resolver = new ObligatoryLayersResolver<>(obligatoryLayers);
      }
      return reader.execute(rf, XyzFeatureCodecFactory.get(), new MergeByStoragePriority<>(), resolver);
    }
  }

  private ViewLayerCollection prepareViewLayerCollection(IStorage nhStorage, List<String> spaceIds) {

    final List<ViewLayer> viewLayerList = new ArrayList<>();
    for (final String spaceId : spaceIds) {
      viewLayerList.add(new ViewLayer(nhStorage, spaceId));
    }

    return new ViewLayerCollection("", viewLayerList);
  }

  private Set<ViewLayer> getObligatoryLayers(ViewLayerCollection viewLayerCollection) {

    int layerCollectionSize = viewLayerCollection.getLayers().size();

    if (layerCollectionSize >= 2) {
      List<ViewLayer> obligatoryLayers = viewLayerCollection.getLayers().subList(0, layerCollectionSize - 1);
      return new HashSet<>(obligatoryLayers);
    } else {
      return Set.of(viewLayerCollection.getTopPriorityLayer());
    }
  }
}
