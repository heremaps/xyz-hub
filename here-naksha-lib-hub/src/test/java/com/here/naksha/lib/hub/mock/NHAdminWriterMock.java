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
import static com.here.naksha.lib.core.models.storage.EWriteOp.CREATE;
import static com.here.naksha.lib.core.models.storage.EWriteOp.DELETE;
import static com.here.naksha.lib.core.models.storage.EWriteOp.PURGE;
import static com.here.naksha.lib.core.models.storage.EWriteOp.PUT;
import static com.here.naksha.lib.core.models.storage.EWriteOp.UPDATE;

import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzCodecFactory;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.psql.EPsqlState;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PSQLState;

public class NHAdminWriterMock extends NHAdminReaderMock implements IWriteSession {

  public NHAdminWriterMock(final @NotNull Map<String, TreeMap<String, Object>> mockCollection) {
    super(mockCollection);
  }

  /**
   * Execute the given write-request.
   *
   * @param writeRequest the write-request to execute.
   * @return the result.
   */
  @Override
  public @NotNull Result execute(@NotNull WriteRequest<?, ?, ?> writeRequest) {
    if (writeRequest instanceof WriteXyzCollections wc) {
      return executeWriteCollection(wc);
    } else if (writeRequest instanceof WriteXyzFeatures wf) {
      return executeWriteFeatures(wf);
    }
    return new ErrorResult(
        XyzError.NOT_IMPLEMENTED,
        "WriteRequest type " + writeRequest.getClass().getName() + " not supported");
  }

  protected @NotNull Result executeWriteCollection(@NotNull WriteXyzCollections wc) {
    final @NotNull List<XyzCollectionCodec> results = new ArrayList<>();
    for (final XyzCollectionCodec collectionCodec : wc.features) {
      // persist collection (if not already)
      EExecutedOp execOp = EExecutedOp.RETAINED;
      if (mockCollection.putIfAbsent(collectionCodec.getFeature().getId(), new TreeMap<>()) == null) {
        execOp = EExecutedOp.CREATED;
      }
      collectionCodec.setOp(execOp);
      // add to output list
      results.add(collectionCodec);
    }
    return new MockResult(XyzCollection.class, results);
  }

  protected @NotNull Result executeWriteFeatures(@NotNull WriteXyzFeatures wf) {
    final @NotNull List<XyzFeatureCodec> results = new ArrayList<>();
    // Raise exception if collection doesn't exist already
    if (mockCollection.get(wf.getCollectionId()) == null) {
      throw unchecked(new SQLException(
          "Collection " + wf.getCollectionId() + " doesn't exist.",
          EPsqlState.COLLECTION_DOES_NOT_EXIST.toString()));
    }
    // Perform write operation for each feature
    for (final XyzFeatureCodec featureCodec : wf.features) {
      // generate new feature Id, if not already provided
      final EWriteOp op = EWriteOp.get(featureCodec.getOp());
      try {
        if ((op == DELETE) && (featureCodec.getFeature() == null)) {
          if (featureCodec.getId() == null) {
            throw new SQLException("Feature to delete has null id!");
          }
          results.add(deleteFeature(wf.getCollectionId(), featureCodec.getId(), featureCodec.getUuid()));
          continue;
        }
        final XyzFeature feature = Objects.requireNonNull(featureCodec.getFeature(), "Codec's feature is null");
        if (op == CREATE && (feature.getId() == null || feature.getId().isEmpty())) {
          feature.setId(UUID.randomUUID().toString());
        }
        // persist feature in a space

        XyzFeatureCodec result;
        if (op.equals(CREATE)) {
          result = insertFeature(wf.getCollectionId(), feature);
        } else if (op.equals(UPDATE)) {
          result = updateFeature(wf.getCollectionId(), feature);
        } else if (op.equals(PUT)) {
          result = upsertFeature(wf.getCollectionId(), feature);
        } else if (op.equals(DELETE)) {
          result = deleteFeature(wf.getCollectionId(), feature);
        } else if (op.equals(PURGE)) {
          return new ErrorResult(XyzError.NOT_IMPLEMENTED, "PurgeFeature not mocked yet");
        } else {
          return new ErrorResult(XyzError.NOT_IMPLEMENTED, op + " not mocked yet");
        }
        // add to output list
        results.add(result);
      } catch (SQLException ex) {
        return mapExceptionToErrorResult(ex);
      }
    }
    return new MockResult<>(XyzFeature.class, results);
  }

  protected @NotNull ErrorResult mapExceptionToErrorResult(final @NotNull SQLException sqe) {
    XyzError error = XyzError.EXCEPTION;
    final String state = sqe.getSQLState();
    if (PSQLState.UNIQUE_VIOLATION.getState().equals(state)) {
      error = XyzError.CONFLICT;
    } else if (PSQLState.NO_DATA.getState().equals(state)) {
      error = XyzError.NOT_FOUND;
    }
    return new ErrorResult(error, sqe.getMessage(), sqe);
  }

  protected <T> @NotNull XyzFeatureCodec insertFeature(
      final @NotNull String collectionId, final @NotNull XyzFeature feature) throws SQLException {
    if (mockCollection.get(collectionId).putIfAbsent(feature.getId(), setUuidFor(feature)) == null) {
      return featureCodec(feature, EExecutedOp.CREATED);
    }
    throw new SQLException("Feature already exists " + feature.getId(), PSQLState.UNIQUE_VIOLATION.getState());
  }

  protected @NotNull XyzFeatureCodec updateFeature(
      final @NotNull String collectionId, final @NotNull XyzFeature feature) throws SQLException {
    final AtomicReference<XyzFeatureCodec> result = new AtomicReference<>();
    final AtomicReference<SQLException> exception = new AtomicReference<>();

    mockCollection.get(collectionId).compute(feature.getId(), (fId, oldF) -> {
      // no existing feature to update
      if (oldF == null) {
        exception.set(new SQLException("No feature found for id " + fId, PSQLState.NO_DATA.getState()));
        return oldF;
      }
      // update if UUID matches (or overwrite if new uuid is missing)
      final XyzFeature ef = (XyzFeature) oldF;
      if ((Objects.equals(uuidOf(ef), uuidOf(feature)) && uuidOf(feature) != null) || uuidOf(feature) == null) {
        result.set(featureCodec(feature, EExecutedOp.UPDATED));
        return setUuidFor(feature);
      } else {
        // throw error if UUID mismatches
        exception.set(new SQLException(
            "Uuid " + uuidOf(ef) + " mismatch for id " + fId,
            EPsqlState.COLLECTION_DOES_NOT_EXIST.toString()));
        return oldF;
      }
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return result.get();
  }

  protected @NotNull XyzFeatureCodec upsertFeature(
      final @NotNull String collectionId, final @NotNull XyzFeature feature) throws SQLException {
    final AtomicReference<XyzFeatureCodec> result = new AtomicReference<>();
    final AtomicReference<SQLException> exception = new AtomicReference<>();

    mockCollection.get(collectionId).compute(feature.getId(), (fId, oldF) -> {
      // insert if missing
      if (oldF == null) {
        if (uuidOf(feature) == null) {
          setUuidFor(feature);
        }
        result.set(featureCodec(feature, EExecutedOp.CREATED));
        return feature;
      }
      // update if UUID matches (or overwrite if new uuid is missing)
      final XyzFeature ef = (XyzFeature) oldF;
      if ((Objects.equals(uuidOf(ef), uuidOf(feature)) && uuidOf(feature) != null) || uuidOf(feature) == null) {
        result.set(featureCodec(feature, EExecutedOp.UPDATED));
        return setUuidFor(feature);
      } else {
        // throw error if UUID mismatches
        exception.set(new SQLException(
            "Uuid " + uuidOf(ef) + " mismatch for id " + feature.getId(),
            PSQLState.UNIQUE_VIOLATION.getState()));
        return oldF;
      }
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return result.get();
  }

  protected @NotNull XyzFeatureCodec deleteFeature(
      final @NotNull String collectionId, final @NotNull String id, final @Nullable String uuid)
      throws SQLException {
    final AtomicReference<XyzFeatureCodec> result = new AtomicReference<>();
    final AtomicReference<SQLException> exception = new AtomicReference<>();

    mockCollection.get(collectionId).compute(id, (fId, oldF) -> {
      // nothing to delete if it is already absent
      if (oldF == null) {
        result.set(featureCodec(id, EExecutedOp.RETAINED));
        return oldF;
      }
      // delete if UUID matches
      final XyzFeature ef = (XyzFeature) oldF;
      if ((Objects.equals(uuidOf(ef), uuid)) || (uuid == null)) {
        result.set(featureCodec(ef, EExecutedOp.DELETED));
        return null;
      } else {
        // throw error if UUID mismatches
        exception.set(new SQLException(
            "Uuid " + uuidOf(ef) + " mismatch for id " + id, PSQLState.UNIQUE_VIOLATION.getState()));
        return oldF;
      }
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return result.get();
  }

  protected @NotNull XyzFeatureCodec deleteFeature(
      final @NotNull String collectionId, final @NotNull XyzFeature feature) throws SQLException {
    final String id = feature.getId();
    final String uuid = uuidOf(feature);
    return deleteFeature(collectionId, id, uuid);
  }

  private @Nullable String uuidOf(final @NotNull XyzFeature feature) {
    return feature.getProperties().getXyzNamespace().getUuid();
  }

  private XyzFeature setUuidFor(final @NotNull XyzFeature feature) {
    feature.getProperties().getXyzNamespace().setUuid(UUID.randomUUID().toString());
    return feature;
  }

  private XyzFeatureCodec featureCodec(XyzFeature feature, EExecutedOp op) {
    return XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class)
        .newInstance()
        .withFeature(feature)
        .withOp(op);
  }

  private XyzFeatureCodec featureCodec(String id, EExecutedOp op) {
    return XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class)
        .newInstance()
        .withId(id)
        .withOp(op);
  }

  /**
   * Acquire a lock to a specific feature in the HEAD state.
   *
   * @param collectionId the collection in which the feature is stored.
   * @param featureId    the identifier of the feature to lock.
   * @param timeout      the maximum time to wait for the lock.
   * @param timeUnit     the time-unit in which the wait-time was provided.
   * @return the lock.
   * @throws StorageLockException if the locking failed.
   */
  @Override
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  /**
   * Acquire an advisory lock.
   *
   * @param lockId   the unique identifier of the lock to acquire.
   * @param timeout  the maximum time to wait for the lock.
   * @param timeUnit the time-unit in which the wait-time was provided.
   * @return the lock.
   * @throws StorageLockException if the locking failed.
   */
  @Override
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return null;
  }

  /**
   * Commit all changes.
   * <p>
   * Beware setting {@code autoCloseCursors} to {@code true} is often very suboptimal. To keep cursors alive, most of the time the
   * implementation requires to read all results synchronously from all open cursors in an in-memory cache and to close the underlying
   * network resources. This can lead to {@link OutOfMemoryError}'s or other issues. It is strictly recommended to first read from all open
   * cursors before closing, committing or rolling-back a session.
   *
   * @param autoCloseCursors If {@code true}, all open cursors are closed; otherwise all pending cursors are kept alive.
   */
  @Override
  public void commit(boolean autoCloseCursors) {}

  /**
   * Abort the transaction, revert all pending changes.
   * <p>
   * Beware setting {@code autoCloseCursors} to {@code true} is often very suboptimal. To keep cursors alive, most of the time the
   * implementation requires to read all results synchronously from all open cursors in an in-memory cache and to close the underlying
   * network resources. This can lead to {@link OutOfMemoryError}'s or other issues. It is strictly recommended to first read from all open
   * cursors before closing, committing or rolling-back a session.
   *
   * @param autoCloseCursors If {@code true}, all open cursors are closed; otherwise all pending cursors are kept alive.
   */
  @Override
  public void rollback(boolean autoCloseCursors) {}

  /**
   * Closes the session and, when necessary invokes {@link #rollback(boolean)}.
   * <p>
   * Beware setting {@code autoCloseCursors} to {@code true} is often very suboptimal. To keep cursors alive, most of the time the
   * implementation requires to read all results synchronously from all open cursors in an in-memory cache and to close the underlying
   * network resources. This can lead to {@link OutOfMemoryError}'s or other issues. It is strictly recommended to first read from all open
   * cursors before closing, committing or rolling-back a session.
   *
   * @param autoCloseCursors If {@code true}, all open cursors are closed; otherwise all pending cursors are kept alive.
   */
  @Override
  public void close(boolean autoCloseCursors) {}
}
