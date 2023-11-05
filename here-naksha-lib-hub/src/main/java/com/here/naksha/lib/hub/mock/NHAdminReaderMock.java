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
import static com.here.naksha.lib.core.models.storage.POp.Constants.OP_EQUALS;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PSQLState;

public class NHAdminReaderMock implements IReadSession {

  protected final @NotNull Map<String, Map<String, Object>> mockCollection;

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
    } else if (pOp.op() == OP_EQUALS && pOp.propertyRef() == PRef.id()) {
      // return features by Id from the given collections names
      for (final String collectionName : rf.getCollections()) {
        if (mockCollection.get(collectionName) == null) {
          throw unchecked(new SQLException(
              "Collection " + collectionName + " not found!", PSQLState.UNDEFINED_TABLE.getState()));
        }
        // if feature not found, return empty list
        if (mockCollection.get(collectionName).get(pOp.value()) == null) break;
        features.add(mockCollection.get(collectionName).get(pOp.value()));
      }
    } else if (pOp.op() == Op.OP_OR) {
      final List<POp> pOpList = pOp.children();
      final List<String> ids = new ArrayList<>();
      for (final POp orOp : pOpList) {
        if (orOp.op() == OP_EQUALS && orOp.propertyRef() == PRef.id()) {
          ids.add((String) orOp.value());
        } else {
          // TODO : Operation Not supported
        }
      }
    } else {
      // TODO : Operation Not supported
    }
    return new MockReadResult<>(XyzFeature.class, features);
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
