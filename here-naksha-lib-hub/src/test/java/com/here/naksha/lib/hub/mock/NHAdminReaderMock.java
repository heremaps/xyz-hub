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
package com.here.naksha.lib.hub.mock;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.psql.EPsqlState;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PSQLState;

public class NHAdminReaderMock implements IReadSession {

  protected static @NotNull Map<String, TreeMap<String, Object>> mockCollection;

  public NHAdminReaderMock() {
    throw new UnsupportedOperationException(
        "NHAdminReaderMock storage should not be used"); // comment to use mock in local env
  }

  public NHAdminReaderMock(final @NotNull Map<String, TreeMap<String, Object>> mockCollection) {
    this.mockCollection = mockCollection;
  }

  /**
   * Tests whether this session is connected to the master-node.
   *
   * @return {@code true}, if this session is connected to the master-node; {@code false} otherwise.
   */
  @Override
  public boolean isMasterConnect() {
    return false;
  }

  /**
   * Returns the Naksha context bound to this read-connection.
   *
   * @return the Naksha context bound to this read-connection.
   */
  @Override
  public @NotNull NakshaContext getNakshaContext() {
    return null;
  }

  /**
   * Returns the amount of features to fetch at ones.
   *
   * @return the amount of features to fetch at ones.
   */
  @Override
  public int getFetchSize() {
    throw new NotImplementedException();
  }

  /**
   * Changes the amount of features to fetch at ones.
   *
   * @param size The amount of features to fetch at ones.
   */
  @Override
  public void setFetchSize(int size) {
    throw new NotImplementedException();
  }

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Execute the given read-request.
   *
   * @param readRequest
   * @return the result.
   */
  @Override
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    if (readRequest instanceof ReadFeatures rf) {
      return executeReadFeatures(rf);
    }
    return new ErrorResult(
        XyzError.NOT_IMPLEMENTED,
        "ReadRequest type " + readRequest.getClass().getName() + " not supported");
  }

  protected @NotNull Result executeReadFeatures(@NotNull ReadFeatures rf) {
    final POp pOp = rf.getPropertyOp();
    final List<Object> features = new ArrayList<>();
    if (pOp == null) {
      // return all features from given collection names
      for (final String collectionName : rf.getCollections()) {
        if (mockCollection.get(collectionName) == null) {
          throw unchecked(new SQLException(
              "Collection " + collectionName + " not found!",
              EPsqlState.COLLECTION_DOES_NOT_EXIST.toString()));
        }
        features.addAll(mockCollection.get(collectionName).values());
      }
    } else if (pOp.op() == POpType.EQ && pOp.getPropertyRef() == PRef.id()) {
      // return features by Id from the given collections names
      getFeatureById(rf.getCollections(), (String) pOp.getValue(), features);
    } else if (pOp.op() == OpType.OR) {
      final List<POp> pOpList = pOp.children();
      for (final POp orOp : pOpList) {
        if (orOp.op() == POpType.EQ && orOp.getPropertyRef() == PRef.id()) {
          getFeatureById(rf.getCollections(), (String) orOp.getValue(), features);
        } else if (orOp.op() == POpType.EXISTS
            && orOp.getPropertyRef().getPath().size() == 3
            && orOp.getPropertyRef().getPath().get(2).equals("tags")) {
          getFeaturesByTagOrOperation(rf.getCollections(), pOp, features);
          break;
        } else if (orOp.op() == OpType.AND) {
          if (orOp.children().get(0).op() == POpType.EXISTS
              && orOp.children().get(0).getPropertyRef().getPath().size() == 3
              && orOp.children()
                  .get(0)
                  .getPropertyRef()
                  .getPath()
                  .get(2)
                  .equals("tags")) {
            getFeaturesByTagAndOperation(rf.getCollections(), orOp, features);
          } else {
            // TODO : Operation Not supported
          }
        } else {
          // TODO : Operation Not supported
        }
      }
    } else if (pOp.op() == POpType.EXISTS) {
      if (pOp.getPropertyRef().getPath().size() == 3
          && pOp.getPropertyRef().getPath().get(2).equals("tags")) {
        getFeatureByTag(rf.getCollections(), pOp.getPropertyRef().getTagName(), features);
      } else {
        // TODO : Operation Not supported
      }
    } else if (pOp.op() == OpType.AND) {
      final List<POp> pOpList = pOp.children();
      for (final POp andOp : pOpList) {
        if (andOp.op() == POpType.EXISTS
            && andOp.getPropertyRef().getPath().size() == 3
            && andOp.getPropertyRef().getPath().get(2).equals("tags")) {
          getFeaturesByTagAndOperation(rf.getCollections(), pOp, features);
          break;
        } else {
          // TODO : Operation Not supported
        }
      }
    } else {
      // TODO : Operation Not supported
    }

    // Apply Spatial operation if was requested
    final SOp sOp = rf.getSpatialOp();
    List<Object> spatialFilteredFeatures = null;
    if (sOp != null) {
      if (sOp.op() == SOpType.INTERSECTS) {
        spatialFilteredFeatures = features.stream()
            .filter(feature -> ((XyzFeature) feature)
                .getGeometry()
                .getJTSGeometry()
                .intersects(sOp.getGeometry()))
            .toList();
        features.clear();
        features.addAll(spatialFilteredFeatures);
      } else {
        // TODO : Operation not supported
      }
    }

    XyzFeatureCodecFactory codecFactory = XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class);
    List<XyzFeatureCodec> featuresAsXyzCodecs = features.stream()
        .map(feature -> codecFactory.newInstance().withFeature((XyzFeature) feature))
        .toList();
    return new MockResult<>(XyzFeature.class, featuresAsXyzCodecs);
  }

  private void getFeatureById(
      final List<String> collectionNames, final String featureId, final List<Object> features) {
    // fetch features for given input OR criteria
    for (final String collectionName : collectionNames) {
      if (mockCollection.get(collectionName) == null) {
        throw unchecked(new SQLException(
            "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
      }
      if (featureId != null) {
        // fetch for given id
        final Object feature = mockCollection.get(collectionName).get(featureId);
        if (feature != null) features.add(feature);
      }
    }
  }

  private void getFeatureByTag(final List<String> collectionNames, final String tag, final List<Object> features) {
    // fetch features for given input OR criteria
    for (final String collectionName : collectionNames) {
      if (mockCollection.get(collectionName) == null) {
        throw unchecked(new SQLException(
            "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
      }
      if (tag != null) {
        // fetch for given tag
        for (Object obj : mockCollection.get(collectionName).values()) {
          XyzFeature feature = (XyzFeature) obj;
          if (feature.getProperties().getXyzNamespace().getTags() != null
              && feature.getProperties()
                  .getXyzNamespace()
                  .getTags()
                  .contains(tag)) {
            if (!features.contains(feature)) {
              features.add(feature);
            }
          }
        }
      }
    }
  }

  private void getFeaturesByTagOrOperation(
      final List<String> collectionNames, final POp orOp, final List<Object> features) {
    // fetch features for given input OR criteria
    for (final String collectionName : collectionNames) {
      if (mockCollection.get(collectionName) == null) {
        throw unchecked(new SQLException(
            "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
      }
      // for each available feature apply AND / OR condition on tag recursively
      for (Object obj : mockCollection.get(collectionName).values()) {
        XyzFeature feature = matchFeatureAgainstTagOrOperations((XyzFeature) obj, orOp);
        if (feature != null) {
          if (!features.contains(feature)) {
            features.add(feature);
          }
        }
      }
    }
  }

  private void getFeaturesByTagAndOperation(
      final List<String> collectionNames, final POp andOp, final List<Object> features) {
    // fetch features for given input OR criteria
    for (final String collectionName : collectionNames) {
      if (mockCollection.get(collectionName) == null) {
        throw unchecked(new SQLException(
            "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
      }
      // for each available feature apply AND / OR condition on tag recursively
      for (Object obj : mockCollection.get(collectionName).values()) {
        XyzFeature feature = matchFeatureAgainstTagAndOperations((XyzFeature) obj, andOp);
        if (feature != null) {
          if (!features.contains(feature)) {
            features.add(feature);
          }
        }
      }
    }
  }

  private XyzFeature matchFeatureAgainstTagAndOperations(final XyzFeature feature, final POp andOp) {
    // fetch features for given input AND criteria
    // so return feature as null, as soon as even one AND condition mismatch
    for (final POp tagOp : andOp.children()) {
      if (tagOp.op() == POpType.EXISTS
          && tagOp.getPropertyRef().getPath().size() == 3
          && tagOp.getPropertyRef().getPath().get(2).equals("tags")) {
        if (feature.getProperties().getXyzNamespace().getTags() == null
            || !feature.getProperties()
                .getXyzNamespace()
                .getTags()
                .contains(tagOp.getPropertyRef().getTagName())) {
          return null;
        }
      } else if (tagOp.op() == POpType.AND) {
        XyzFeature matchingFeature = matchFeatureAgainstTagAndOperations(feature, tagOp);
        if (matchingFeature == null) return null;
      } else if (tagOp.op() == POpType.OR) {
        XyzFeature matchingFeature = matchFeatureAgainstTagOrOperations(feature, tagOp);
        if (matchingFeature == null) return null;
      } else {
        return null;
      }
    }
    return feature;
  }

  private XyzFeature matchFeatureAgainstTagOrOperations(final XyzFeature feature, final POp orOp) {
    // fetch features for given input OR criteria
    // so return feature as null, as soon as even one OR condition match
    for (final POp tagOp : orOp.children()) {
      if (tagOp.op() == POpType.EXISTS
          && tagOp.getPropertyRef().getPath().size() == 3
          && tagOp.getPropertyRef().getPath().get(2).equals("tags")) {
        if (feature.getProperties().getXyzNamespace().getTags() != null
            && feature.getProperties()
                .getXyzNamespace()
                .getTags()
                .contains(tagOp.getPropertyRef().getTagName())) {
          return feature;
        }
      } else if (tagOp.op() == POpType.AND) {
        XyzFeature matchingFeature = matchFeatureAgainstTagAndOperations(feature, tagOp);
        if (matchingFeature != null) return matchingFeature;
      } else if (tagOp.op() == POpType.OR) {
        XyzFeature matchingFeature = matchFeatureAgainstTagOrOperations(feature, tagOp);
        if (matchingFeature != null) return matchingFeature;
      } else {
        return null;
      }
    }
    return null;
  }

  /**
   * Process the given notification.
   *
   * @param notification
   * @return the result.
   */
  @Override
  public @NotNull Result process(@NotNull Notification<?> notification) {
    return null;
  }

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @Override
  public void close() {}
}
