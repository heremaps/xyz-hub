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
package com.here.naksha.lib.view;

import static com.here.naksha.lib.core.util.storage.RequestHelper.readFeaturesByIdsRequest;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.FeatureCodec;
import com.here.naksha.lib.core.models.storage.FeatureCodecFactory;
import com.here.naksha.lib.core.models.storage.HeapCacheCursor;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.ISession;
import com.here.naksha.lib.view.concurrent.LayerReadRequest;
import com.here.naksha.lib.view.concurrent.ParallelQueryExecutor;
import com.here.naksha.lib.view.merge.MergeByStoragePriority;
import com.here.naksha.lib.view.missing.ObligatoryLayersResolver;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link  ViewReadSession} operates on {@link View}, it queries simultaneously all the storages.
 * Then it tries to fetch missing features {@link MissingIdResolver} if needed.
 * At the end {@link MergeOperation} is executed and single result returned.
 * You can provide your own merge operation. The default is "take result from storage on the top". <br>
 *
 * <strong>Important:</strong> {@link ViewReadSession} will always return mutable cursor, this is the only way we can
 * merge results from different storages and fetch missing by ids. Consider this example:
 * Result from Storage A: [F_1, F_2, F_3, F_4]
 * Result from Storage B: [F_2, F_4]
 * Result from Storage C: [F_3, F_5]
 * In this situation using Forward cursor would lead to N+1 issue, as after reading 1st row from each result we'd have
 * to fetch missing F_1 from B and C.
 * To be able to create query that fetches multiple missing features we have to know them first (by caching ahead of time) <br>
 * <p>
 * It might happen that feature has been moved (it's geometry changed). In such case after getting results for bbox
 * query we have to query again for all features (by id) that was missing in a least one storage  result.
 */
public class ViewReadSession implements IReadSession {

  protected final View viewRef;

  protected ParallelQueryExecutor parallelQueryExecutor;

  protected Map<ViewLayer, IReadSession> subSessions;

  protected ViewReadSession(@NotNull View viewRef, @Nullable NakshaContext context, boolean useMaster) {
    this.viewRef = viewRef;
    this.subSessions = new LinkedHashMap<>();
    for (ViewLayer layer : viewRef.getViewCollection().getLayers()) {
      subSessions.put(layer, layer.getStorage().newReadSession(context, useMaster));
    }
    this.parallelQueryExecutor = new ParallelQueryExecutor(viewRef);
  }

  @Override
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    return execute(
        readRequest,
        XyzFeatureCodecFactory.get(),
        new MergeByStoragePriority<>(),
        new ObligatoryLayersResolver<>(
            Set.of(viewRef.getViewCollection().getTopPriorityLayer())));
  }

  public <FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>> Result execute(
      @NotNull ReadRequest<?> request,
      FeatureCodecFactory<FEATURE, CODEC> codecFactory,
      @NotNull MergeOperation<FEATURE, CODEC> mergeOperation,
      @NotNull MissingIdResolver<FEATURE, CODEC> missingIdResolver) {

    if (!(request instanceof ReadFeatures)) {
      throw new UnsupportedOperationException("Only ReadFeatures are supported.");
    }

    /*
    Call every layer/storage and get the first result.
    After that we should have multiLayerRows like that:
    [
    <featureId_1, [Layer0_Feature1, Layer1_Feature1, ... LayerN_Feature1]>,
    <featureId_2, [Layer0_Feature2, Layer1_Feature2, ... LayerN_Feature2]>,
    ...
    ]
     */
    List<LayerReadRequest> layerReadRequests = subSessions.entrySet().stream()
        .map(entry -> new LayerReadRequest((ReadFeatures) request, entry.getKey(), entry.getValue()))
        .collect(toList());
    Map<String, List<ViewLayerRow<FEATURE, CODEC>>> multiLayerRows =
        parallelQueryExecutor.queryInParallel(layerReadRequests, codecFactory);

    /*
    If one of the features is missing on one or few layers, we use getMissingFeatures and missingIdResolver to try to fetch it again by id.
    I.e. when we made a request in the first step to Layer0, Layer1 and Layer2, but we got feature only from Layer0 and Layer2:
    [
    <featureId_1, [Layer0_Feature1, Layer2_Feature1]>
    ]
    then missingIdResolver may decide to create another request to Layer1 querying by featureId_1.
    So the result of getMissingFeatures(..) would look like this:
    [
    <featureId_1, [Layer1_Feature1]>
    ]
    or it might be empty if feature is not there
     */
    Map<String, List<ViewLayerRow<FEATURE, CODEC>>> fetchedById = isRequestOnlyById(request)
        ? Collections.emptyMap()
        : getMissingFeatures(multiLayerRows, missingIdResolver, codecFactory);

    /*
    putting all together:
    [ <featureId_1, [Layer0_Feature1, Layer2_Feature1]> ]
    and
    [ <featureId_1, [Layer1_Feature1]> ]
    to get:
    [ <featureId_1, [Layer0_Feature1, Layer1_Feature1, Layer2_Feature1]> ]
     */
    fetchedById.forEach((key, value) -> multiLayerRows.get(key).addAll(value));

    /*
    Merging: [ <featureId_1, [Layer0_Feature1, Layer1_Feature1, Layer2_Feature1]> ]
    into final result:  [ Feature1 ]
     */
    List<CODEC> mergedRows =
        multiLayerRows.values().stream().map(mergeOperation::apply).collect(toList());

    HeapCacheCursor<FEATURE, CODEC> heapCacheCursor = new HeapCacheCursor<>(codecFactory, mergedRows, null);

    return new ViewSuccessResult(heapCacheCursor, null);
  }

  private <FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
      Map<String, List<ViewLayerRow<FEATURE, CODEC>>> getMissingFeatures(
          @NotNull Map<String, List<ViewLayerRow<FEATURE, CODEC>>> multiLayerRows,
          @NotNull MissingIdResolver<FEATURE, CODEC> missingIdResolver,
          @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory) {

    Map<String, List<ViewLayerRow<FEATURE, CODEC>>> result = new HashMap<>();
    if (!missingIdResolver.skip()) {
      // Prepare map of <Layer_x, [FeatureId_x, ..., FeatureId_z]> features and layers you want to search by id.
      // to query only once each layer
      Map<ViewLayer, List<String>> idsToFetch = multiLayerRows.values().stream()
          .map(missingIdResolver::layersToSearch)
          .filter(Objects::nonNull)
          .flatMap(Collection::stream)
          .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));

      // Prepare request by id and query given layers.
      List<LayerReadRequest> missingFeaturesRequests = idsToFetch.entrySet().stream()
          .map(entry -> new LayerReadRequest(
              readFeaturesByIdsRequest(entry.getKey().getCollectionId(), entry.getValue()),
              entry.getKey(),
              subSessions.get(entry.getKey())))
          .collect(toList());

      result = parallelQueryExecutor.queryInParallel(missingFeaturesRequests, codecFactory);
    }
    return result;
  }

  @Override
  public boolean isMasterConnect() {
    return false;
  }

  @Override
  public @NotNull NakshaContext getNakshaContext() {
    return subSessions.values().stream()
        .findAny()
        .map(IReadSession::getNakshaContext)
        .orElseThrow();
  }

  @Override
  public int getFetchSize() {
    return subSessions.values().stream()
        .findAny()
        .map(IReadSession::getFetchSize)
        .orElseThrow();
  }

  @Override
  public void setFetchSize(int size) {
    subSessions.values().forEach(session -> session.setFetchSize(size));
  }

  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return subSessions.values().stream()
        .findAny()
        .map(session -> session.getStatementTimeout(timeUnit))
        .orElseThrow();
  }

  @Override
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    subSessions.values().forEach(session -> session.setStatementTimeout(timeout, timeUnit));
  }

  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return subSessions.values().stream()
        .findAny()
        .map(session -> session.getLockTimeout(timeUnit))
        .orElseThrow();
  }

  @Override
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    subSessions.values().forEach(session -> session.setLockTimeout(timeout, timeUnit));
  }

  @Override
  public @NotNull Result process(@NotNull Notification<?> notification) {
    return new ErrorResult(XyzError.NOT_IMPLEMENTED, "process");
  }

  @Override
  public void close() {
    subSessions.values().forEach(ISession::close);
  }

  private boolean isRequestOnlyById(ReadRequest<?> request) {
    if (request instanceof ReadFeatures) {
      ReadFeatures readFeatures = (ReadFeatures) request;
      POp propertyOp = readFeatures.getPropertyOp();
      return readFeatures.getSpatialOp() == null && isPropertyOpIdOnly(propertyOp);
    } else {
      return false;
    }
  }

  private boolean isPropertyOpIdOnly(POp pOp) {
    if (pOp == null) {
      return false;
    }
    if (pOp.children() == null) {
      return pOp.op() == POpType.EQ && PRef.id().equals(pOp.getPropertyRef());
    } else {
      return pOp.children().stream().allMatch(this::isPropertyOpIdOnly);
    }
  }
}
