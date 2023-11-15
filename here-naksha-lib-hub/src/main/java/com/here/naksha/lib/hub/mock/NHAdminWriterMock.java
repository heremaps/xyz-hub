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
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteOp;
import com.here.naksha.lib.core.models.storage.WriteOpResult;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PSQLState;

public class NHAdminWriterMock extends NHAdminReaderMock implements IWriteSession {

  public NHAdminWriterMock(final @NotNull Map<String, Map<String, Object>> mockCollection) {
    super(mockCollection);
  }

  /**
   * Execute the given write-request.
   *
   * @param writeRequest the write-request to execute.
   * @return the result.
   */
  @Override
  public @NotNull Result execute(@NotNull WriteRequest writeRequest) {
    if (writeRequest instanceof WriteCollections<?> wc) {
      return executeWriteCollection(wc);
    } else if (writeRequest instanceof WriteFeatures<?> wf) {
      return executeWriteFeatures(wf);
    }
    return new ErrorResult(
        XyzError.NOT_IMPLEMENTED,
        "WriteRequest type " + writeRequest.getClass().getName() + " not supported");
  }

  protected @NotNull <T extends NakshaFeature> Result executeWriteCollection(@NotNull WriteCollections<T> wc) {
    final @NotNull List<WriteOpResult<XyzCollection>> wOpResults = new ArrayList<>();
    for (final WriteOp<T> wOp : wc.queries) {
      final XyzCollection c = (XyzCollection) wOp.feature;
      // persist collection (if not already)
      EExecutedOp execOp = EExecutedOp.RETAIN;
      if (mockCollection.putIfAbsent(c.getId(), new ConcurrentHashMap<>()) == null) {
        execOp = EExecutedOp.CREATED;
      }
      // add to output list
      wOpResults.add(new WriteOpResult<>(execOp, c));
    }
    return new MockWriteResult<>(XyzCollection.class, wOpResults);
  }

  protected @NotNull <T> Result executeWriteFeatures(@NotNull WriteFeatures<T> wf) {
    final @NotNull List<WriteOpResult<XyzFeature>> wOpResults = new ArrayList<>();
    // Raise exception if collection doesn't exist already
    if (mockCollection.get(wf.getCollectionId()) == null) {
      throw unchecked(new SQLException(
          "Collection " + wf.getCollectionId() + " doesn't exist.", PSQLState.UNDEFINED_TABLE.getState()));
    }
    // Perform write operation for each feature
    for (final WriteOp<T> wOp : wf.queries) {
      // generate new feature Id, if not already provided
      final XyzFeature f = (XyzFeature) wOp.feature;
      if (wOp.op == CREATE && (f.getId() == null || f.getId().isEmpty())) {
        f.setId(UUID.randomUUID().toString());
      }
      // persist feature in a space
      try {
        WriteOpResult<XyzFeature> result = null;
        if (wOp.op.equals(CREATE)) {
          result = insertFeature(wf.getCollectionId(), wOp);
        } else if (wOp.op.equals(UPDATE)) {
          result = updateFeature(wf.getCollectionId(), wOp);
        } else if (wOp.op.equals(PUT)) {
          result = upsertFeature(wf.getCollectionId(), wOp);
        } else if (wOp.op.equals(DELETE)) {
          result = deleteFeature(wf.getCollectionId(), wOp);
        } else if (wOp.op.equals(PURGE)) {
          return new ErrorResult(XyzError.NOT_IMPLEMENTED, "PurgeFeature not mocked yet");
        } else {
          return new ErrorResult(XyzError.NOT_IMPLEMENTED, wOp.op + " not mocked yet");
        }
        // add to output list
        wOpResults.add(result);
      } catch (SQLException ex) {
        return mapExceptionToErrorResult(ex);
      }
    }
    return new MockWriteResult<>(XyzFeature.class, wOpResults);
  }

  protected @NotNull ErrorResult mapExceptionToErrorResult(final @NotNull SQLException sqe) {
    XyzError error = XyzError.EXCEPTION;
    final String state = sqe.getSQLState();
    if (PSQLState.UNIQUE_VIOLATION.getState().equals(state)) {
      error = XyzError.CONFLICT;
    }
    return new ErrorResult(error, sqe.getMessage(), sqe);
  }

  protected <T> @NotNull WriteOpResult<XyzFeature> insertFeature(
      final @NotNull String collectionId, final @NotNull WriteOp<T> wOp) throws SQLException {
    final XyzFeature f = (XyzFeature) wOp.feature;
    if (mockCollection.get(collectionId).putIfAbsent(f.getId(), setUuidFor(f)) == null) {
      return new WriteOpResult<>(EExecutedOp.CREATED, f);
    }
    throw new SQLException("Feature already exists " + f.getId(), PSQLState.UNIQUE_VIOLATION.getState());
  }

  protected @NotNull <T> WriteOpResult<XyzFeature> updateFeature(
      final @NotNull String collectionId, final @NotNull WriteOp<T> wOp) throws SQLException {
    final XyzFeature newF = (XyzFeature) wOp.feature;
    final AtomicReference<WriteOpResult<XyzFeature>> result = new AtomicReference<>();
    final AtomicReference<SQLException> exception = new AtomicReference<>();

    mockCollection.get(collectionId).compute(newF.getId(), (fId, oldF) -> {
      // no existing feature to update
      if (oldF == null) {
        exception.set(
            new SQLException("No feature found for id " + fId, PSQLState.UNIQUE_VIOLATION.getState()));
        return oldF;
      }
      // update if UUID matches (or overwrite if new uuid is missing)
      final XyzFeature ef = (XyzFeature) oldF;
      if (uuidOf(ef).equals(uuidOf(newF)) || uuidOf(newF) == null) {
        result.set(new WriteOpResult<>(EExecutedOp.UPDATED, setUuidFor(newF)));
        return newF;
      } else {
        // throw error if UUID mismatches
        exception.set(new SQLException(
            "Uuid " + uuidOf(ef) + " mismatch for id " + fId, PSQLState.UNIQUE_VIOLATION.getState()));
        return oldF;
      }
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return result.get();
  }

  protected @NotNull <T> WriteOpResult<XyzFeature> upsertFeature(
      final @NotNull String collectionId, final @NotNull WriteOp<T> wOp) throws SQLException {
    final XyzFeature newF = (XyzFeature) wOp.feature;
    final AtomicReference<WriteOpResult<XyzFeature>> result = new AtomicReference<>();
    final AtomicReference<SQLException> exception = new AtomicReference<>();

    mockCollection.get(collectionId).compute(newF.getId(), (fId, oldF) -> {
      // insert if missing
      if (oldF == null) {
        result.set(new WriteOpResult<>(EExecutedOp.CREATED, setUuidFor(newF)));
        return newF;
      }
      // update if UUID matches (or overwrite if new uuid is missing)
      final XyzFeature ef = (XyzFeature) oldF;
      if (uuidOf(ef).equals(uuidOf(newF)) || uuidOf(newF) == null) {
        result.set(new WriteOpResult<>(EExecutedOp.UPDATED, setUuidFor(newF)));
        return newF;
      } else {
        // throw error if UUID mismatches
        exception.set(new SQLException(
            "Uuid " + uuidOf(ef) + " mismatch for id " + newF.getId(),
            PSQLState.UNIQUE_VIOLATION.getState()));
        return oldF;
      }
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return result.get();
  }

  protected @NotNull <T> WriteOpResult<XyzFeature> deleteFeature(
      final @NotNull String collectionId, final @NotNull WriteOp<T> wOp) throws SQLException {
    final XyzFeature newF = (wOp.feature != null) ? ((XyzFeature) wOp.feature) : null;
    final String id = (newF != null) ? newF.getId() : wOp.id;
    final String uuid = (newF != null) ? uuidOf(newF) : wOp.uuid;
    final AtomicReference<WriteOpResult<XyzFeature>> result = new AtomicReference<>();
    final AtomicReference<SQLException> exception = new AtomicReference<>();

    mockCollection.get(collectionId).compute(id, (fId, oldF) -> {
      // nothing to delete if it is already absent
      if (oldF == null) {
        result.set(new WriteOpResult<>(EExecutedOp.DELETED, null));
        return oldF;
      }
      // delete if UUID matches
      final XyzFeature ef = (XyzFeature) oldF;
      if (uuidOf(ef).equals(uuid)) {
        result.set(new WriteOpResult<>(EExecutedOp.DELETED, ef));
        return null;
      } else {
        // throw error if UUID mismatches
        exception.set(new SQLException(
            "Uuid " + uuidOf(ef) + " mismatch for id " + newF.getId(),
            PSQLState.UNIQUE_VIOLATION.getState()));
        return oldF;
      }
    });
    if (exception.get() != null) {
      throw exception.get();
    }
    return result.get();
  }

  private @Nullable String uuidOf(final @NotNull XyzFeature feature) {
    return feature.getProperties().getXyzNamespace().getUuid();
  }

  private XyzFeature setUuidFor(final @NotNull XyzFeature feature) {
    feature.getProperties().getXyzNamespace().setUuid(UUID.randomUUID().toString());
    return feature;
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
   */
  @Override
  public void commit() {}

  /**
   * Abort the transaction, revert all pending changes.
   */
  @Override
  public void rollback() {}
}
