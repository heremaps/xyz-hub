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
import static com.here.naksha.lib.core.models.storage.PRef.PATH_TO_PREF_MAPPING;
import static com.here.naksha.lib.core.models.storage.PRef.id;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.coordinates.MultiPointCoordinates;
import com.here.naksha.lib.core.models.geojson.coordinates.PointCoordinates;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.*;
import com.vividsolutions.jts.geom.Geometry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static <FEATURE extends XyzFeature> @NotNull WriteXyzFeatures createFeatureRequest(
      final @NotNull String collectionName, final @NotNull FEATURE feature, final boolean silentIfExists) {
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
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static <FEATURE extends XyzFeature> @NotNull WriteXyzFeatures createFeatureRequest(
      final @NotNull String collectionName, final @NotNull FEATURE feature) {
    return createFeaturesRequest(collectionName, List.of(feature), IfExists.FAIL, IfConflict.FAIL);
  }

  /**
   * Helper method to create WriteFeatures request for updating given feature.
   *
   * @param collectionName name of the storage collection
   * @param feature        feature object to be updated
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  public static <FEATURE extends XyzFeature> @NotNull WriteXyzFeatures updateFeatureRequest(
      final @NotNull String collectionName, final @NotNull FEATURE feature) {
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionName);
    request.update(feature);
    return request;
  }

  /**
   * Helper method to create WriteFeatures request for updating multiple features.
   *
   * @param collectionName name of the storage collection
   * @param features       feature object array to be updated
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  public static @NotNull <FEATURE extends XyzFeature> WriteXyzFeatures updateFeaturesRequest(
      final @NotNull String collectionName, final @NotNull List<FEATURE> features) {
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionName);
    for (FEATURE feature : features) {
      request.update(feature);
    }
    return request;
  }

  /**
   * Helper method to create WriteFeatures request for upserting multiple features.
   *
   * @param collectionName name of the storage collection
   * @param features       feature object array to be updated
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  public static @NotNull <FEATURE extends XyzFeature> WriteXyzFeatures upsertFeaturesRequest(
      final @NotNull String collectionName, final @NotNull List<FEATURE> features) {
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionName);
    for (FEATURE feature : features) {
      request.put(feature);
    }
    return request;
  }

  /**
   * Helper method to create WriteFeatures request for deleting multiple features.
   *
   * @param collectionName name of the storage collection
   * @param ids       feature object array to be deleted
   * @return WriteFeatures request that can be used against IStorage methods
   */
  public static @NotNull WriteXyzFeatures deleteFeaturesByIdsRequest(
      final @NotNull String collectionName, final @NotNull List<String> ids) {
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionName);
    for (String id : ids) {
      request.delete(new XyzFeature(id));
    }
    return request;
  }

  /**
   * Helper method to create WriteFeatures request for deleting given feature.
   *
   * @param collectionName name of the storage collection
   * @param id        feature object to be deleted
   * @return WriteFeatures request that can be used against IStorage methods
   */
  public static @NotNull WriteXyzFeatures deleteFeatureByIdRequest(
      final @NotNull String collectionName, final @NotNull String id) {
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionName);
    return request.delete(new XyzFeature(id));
  }

  /**
   * Helper method to create WriteFeatures request with given list of features. If silentIfExists is true, function internally sets
   * IfExists.RETAIN and IfConflict.RETAIN (to silently ignoring create operation, if feature already exists). If set to false, both flags
   * will be set to FAIL, which will ensure that feature doesn't get overwritten in storage, if already exists.
   *
   * @param collectionName name of the storage collection
   * @param featureList    list of features to be created
   * @param silentIfExists flag to turn on/off silent create operation
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static <FEATURE extends XyzFeature> @NotNull WriteXyzFeatures createFeatureRequest(
      final @NotNull String collectionName,
      final @NotNull List<FEATURE> featureList,
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
   * @param <FEATURE>      any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static <FEATURE extends XyzFeature> @NotNull WriteXyzFeatures createFeaturesRequest(
      final @NotNull String collectionName, final @NotNull List<FEATURE> featureList) {
    return createFeaturesRequest(collectionName, featureList, IfExists.FAIL, IfConflict.FAIL);
  }

  /**
   * Helper method to create WriteFeatures request with given feature.
   *
   * @param collectionName   name of the storage collection
   * @param feature          feature object to be created
   * @param ifExistsAction   flag to indicate what to do if feature already found existing in database
   * @param ifConflictAction flag to indicate what to do if feature version in database conflicts with given feature version
   * @param <FEATURE>        any object extending XyzFeature
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static <FEATURE extends XyzFeature> @NotNull WriteXyzFeatures createFeatureRequest(
      final @NotNull String collectionName,
      final @NotNull FEATURE feature,
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
   * @return WriteFeatures request that can be used against IStorage methods
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static @NotNull WriteXyzFeatures createFeaturesRequest(
      final @NotNull String collectionName,
      final @NotNull List<? extends XyzFeature> featureList,
      final @NotNull IfExists ifExistsAction,
      final @NotNull IfConflict ifConflictAction) {
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionName);
    for (final XyzFeature feature : featureList) {
      assert feature != null;
      request.add(EWriteOp.CREATE, feature);
    }
    return request;
  }

  public static @NotNull WriteXyzCollections createWriteCollectionsRequest(
      final @NotNull List<@NotNull String> collectionNames) {
    final WriteXyzCollections writeXyzCollections = new WriteXyzCollections();
    for (final String collectionId : collectionNames) {
      writeXyzCollections.add(EWriteOp.CREATE, new XyzCollection(collectionId));
    }
    return writeXyzCollections;
  }

  /**
   * Helper function that returns Geometry representing BoundingBox for the co-ordinates
   * supplied as arguments.
   *
   * @param west west co-ordinate
   * @param south south co-ordinate
   * @param east east co-ordinate
   * @param north north co-ordinate
   * @return Geometry representing BBox envelope
   */
  public static @NotNull Geometry createBBoxEnvelope(
      final double west, final double south, final double east, final double north) {
    MultiPointCoordinates multiPoint = new MultiPointCoordinates();
    multiPoint.add(new PointCoordinates(west, south));
    multiPoint.add(new PointCoordinates(east, north));
    return JTSHelper.toMultiPoint(multiPoint).getEnvelope();
  }

  /**
   * Helper function that returns instance of PRef or NonIndexedPRef depending on
   * whether the propPath provided matches with standard (indexed) property search or not.
   *
   * @param propPath the JSON path to be used for property search
   * @return PRef instance of PRef or NonIndexedPRef
   */
  public static @NotNull PRef pRefFromPropPath(final @NotNull String[] propPath) {
    // check if we can use standard PRef (on indexed properties)
    for (final String[] path : PATH_TO_PREF_MAPPING.keySet()) {
      if (Arrays.equals(path, propPath)) {
        return PATH_TO_PREF_MAPPING.get(path);
      }
    }
    // fallback to non-standard PRef (non-indexed properties)
    return new NonIndexedPRef(propPath);
  }

  public static void combineOperationsForRequestAs(
      final @NotNull ReadFeatures request, final OpType opType, @Nullable POp... operations) {
    if (operations == null) return;
    List<POp> opList = null;
    for (final POp crtOp : operations) {
      if (crtOp == null) continue;
      if (request.getPropertyOp() == null) {
        request.setPropertyOp(crtOp); // set operation directly if this was the only one operation
        continue;
      } else if (opList == null) {
        opList = new ArrayList<>(); // we have more than one operation
        opList.add(request.getPropertyOp()); // save previously added operation
      }
      opList.add(crtOp); // keep appending every operation that is to be added to the request
    }
    if (opList == null) return;
    // Add combined operations to request
    if (opType == OpType.AND) {
      request.setPropertyOp(POp.and(opList.toArray(POp[]::new)));
    } else {
      request.setPropertyOp(POp.or(opList.toArray(POp[]::new)));
    }
  }
}
