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
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.OpType;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.XyzCodecFactory;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IReadSession;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PSQLState;

public class NHAdminReaderMock implements IReadSession {

  protected static @NotNull Map<String, Map<String, Object>> mockCollection;

  public NHAdminReaderMock(final @NotNull Map<String, Map<String, Object>> mockCollection) {
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
              "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
        }
        features.addAll(mockCollection.get(collectionName).values());
      }
    } else if (pOp.op() == POpType.EQ && pOp.getPropertyRef() == PRef.id()) {
      // return features by Id from the given collections names
      for (final String collectionName : rf.getCollections()) {
        if (mockCollection.get(collectionName) == null) {
          throw unchecked(new SQLException(
              "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
        }
        // if feature not found, return empty list
        if (mockCollection.get(collectionName).get(pOp.getValue()) == null) {
          break;
        }
        features.add(mockCollection.get(collectionName).get(pOp.getValue()));
      }
    } else if (pOp.op() == OpType.OR) {
      final List<POp> pOpList = pOp.children();
      final List<String> ids = new ArrayList<>();
      for (final POp orOp : pOpList) {
        if (orOp.op() == POpType.EQ && orOp.getPropertyRef() == PRef.id()) {
          ids.add((String) orOp.getValue());
        } else {
          // TODO : Operation Not supported
        }
      }
      // fetch features for all given ids
      for (final String collectionName : rf.getCollections()) {
        if (mockCollection.get(collectionName) == null) {
          throw unchecked(new SQLException(
              "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
        }
        features.addAll(ids.stream()
            .map(id -> mockCollection.get(collectionName).get(id))
            .filter(Objects::nonNull)
            .toList());
      }
    } else {
      // TODO : Operation Not supported
    }
    XyzFeatureCodecFactory codecFactory = XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class);
    List<XyzFeatureCodec> featuresAsXyzCodecs = features.stream()
        .map(feature -> codecFactory.newInstance().withFeature((XyzFeature) feature))
        .toList();
    return new MockResult<>(XyzFeature.class, featuresAsXyzCodecs);
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
