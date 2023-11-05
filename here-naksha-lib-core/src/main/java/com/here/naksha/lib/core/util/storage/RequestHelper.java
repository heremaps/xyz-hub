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
package com.here.naksha.lib.core.util.storage;

import static com.here.naksha.lib.core.models.storage.POp.eq;
import static com.here.naksha.lib.core.models.storage.POp.or;
import static com.here.naksha.lib.core.models.storage.PRef.id;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.*;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

@AvailableSince(NakshaVersion.v2_0_7)
public class RequestHelper {

  /**
   * Helper method to create ReadFeatures request for reading feature by given Id from given storage collection name.
   *
   * @param collectionName name of the storage collection
   * @param featureId      id to fetch matching feature
   * @return ReadFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull ReadFeatures readFeaturesByIdRequest(
      final @NotNull String collectionName, final @NotNull String featureId) {
    return new ReadFeatures().addCollection(collectionName).withPropertyOp(eq(id(), featureId));
  }

  /**
   * Helper method to create ReadFeatures request for reading feature by given Ids from given storage collection name.
   *
   * @param collectionName name of the storage collection
   * @param featureIds     list of ids to fetch matching features
   * @return ReadFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull ReadFeatures readFeaturesByIdsRequest(
      final @NotNull String collectionName, final @NotNull List<@NotNull String> featureIds) {
    final POp[] ops = featureIds.stream().map(id -> eq(id(), id)).toArray(POp[]::new);
    return new ReadFeatures().addCollection(collectionName).withPropertyOp(or(ops));
  }

  /**
   * Helper method to create WriteFeatures request with given feature. If silentIfExists is true, function internally sets IfExists.RETAIN
   * and IfConflict.RETAIN (to silently ignoring create operation, if feature already exists). If set to false, both flags will be set to
   * FAIL, which will ensure that feature doesn't get overwritten in storage, if already exists.
   *
   * @param collectionName name of the storage collection
   * @param feature        feature object to be created
   * @param silentIfExists flag to turn on/off silent create operation
   * @param <T>            any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull <T extends XyzFeature> WriteFeatures<T> createFeatureRequest(
      final @NotNull String collectionName, final @NotNull T feature, final boolean silentIfExists) {
    if (silentIfExists) {
      return createFeaturesRequest(collectionName, List.of(feature), IfExists.RETAIN, IfConflict.RETAIN);
    } else {
      return createFeaturesRequest(collectionName, List.of(feature), IfExists.FAIL, IfConflict.FAIL);
    }
  }

  /**
   * Helper method to create WriteFeatures request with given feature. Function internally sets flags IfExists.FAIL and IfConflict.FAIL,
   * which will ensure that feature doesn't get overwritten in storage, if already exists.
   *
   * @param collectionName name of the storage collection
   * @param feature        feature object to be created
   * @param <T>            any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull <T extends XyzFeature> WriteFeatures<T> createFeatureRequest(
      final @NotNull String collectionName, final @NotNull T feature) {
    return createFeaturesRequest(collectionName, List.of(feature), IfExists.FAIL, IfConflict.FAIL);
  }

  /**
   * Helper method to create WriteFeatures request with given list of features. If silentIfExists is true, function internally sets
   * IfExists.RETAIN and IfConflict.RETAIN (to silently ignoring create operation, if feature already exists). If set to false, both flags
   * will be set to FAIL, which will ensure that feature doesn't get overwritten in storage, if already exists.
   *
   * @param collectionName name of the storage collection
   * @param featureList    list of features to be created
   * @param silentIfExists flag to turn on/off silent create operation
   * @param <T>            any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull <T extends XyzFeature> WriteFeatures<T> createFeatureRequest(
      final @NotNull String collectionName,
      final @NotNull List<@NotNull T> featureList,
      final boolean silentIfExists) {
    if (silentIfExists) {
      return createFeaturesRequest(collectionName, featureList, IfExists.RETAIN, IfConflict.RETAIN);
    } else {
      return createFeaturesRequest(collectionName, featureList, IfExists.FAIL, IfConflict.FAIL);
    }
  }

  /**
   * Helper method to create WriteFeatures request with given list of features. Function internally sets flags IfExists.FAIL and
   * IfConflict.FAIL, which will ensure that feature doesn't get overwritten in storage, if already exists.
   *
   * @param collectionName name of the storage collection
   * @param featureList    list of feature objects to be created
   * @param <T>            any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull <T extends XyzFeature> WriteFeatures<T> createFeaturesRequest(
      final @NotNull String collectionName, final @NotNull List<@NotNull T> featureList) {
    return createFeaturesRequest(collectionName, featureList, IfExists.FAIL, IfConflict.FAIL);
  }

  /**
   * Helper method to create WriteFeatures request with given feature.
   *
   * @param collectionName   name of the storage collection
   * @param feature          feature object to be created
   * @param ifExistsAction   flag to indicate what to do if feature already found existing in database
   * @param ifConflictAction flag to indicate what to do if feature version in database conflicts with given feature version
   * @param <T>              any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull <T extends XyzFeature> WriteFeatures<T> createFeatureRequest(
      final @NotNull String collectionName,
      final @NotNull T feature,
      final @NotNull IfExists ifExistsAction,
      final @NotNull IfConflict ifConflictAction) {
    return createFeaturesRequest(collectionName, List.of(feature), ifExistsAction, ifConflictAction);
  }

  /**
   * Helper method to create WriteFeatures request with given list of features.
   *
   * @param collectionName   name of the storage collection
   * @param featureList      list of feature objects to be created
   * @param ifExistsAction   flag to indicate what to do if feature already found existing in database
   * @param ifConflictAction flag to indicate what to do if feature version in database conflicts with given feature version
   * @param <T>              any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull <T extends XyzFeature> WriteFeatures<T> createFeaturesRequest(
      final @NotNull String collectionName,
      final @NotNull List<@NotNull T> featureList,
      final @NotNull IfExists ifExistsAction,
      final @NotNull IfConflict ifConflictAction) {
    // TODO: Due to simplification and not yet supported advanced write ops, please adjust!
    final List<WriteOp<T>> opList = featureList.stream()
        .map(feature -> new WriteOp<>(EWriteOp.INSERT, feature))
        .toList();
    return new WriteFeatures<>(collectionName, opList);
  }

  public static @NotNull WriteCollections<StorageCollection> createWriteCollectionsRequest(
      final @NotNull List<@NotNull String> collectionNames) {
    final List<WriteOp<StorageCollection>> collectionList = collectionNames.stream()
        .map(name -> new WriteOp<>(EWriteOp.INSERT, new StorageCollection(name), false))
        .toList();
    return new WriteCollections<>(collectionList);
  }
}
