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
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PsqlWriteSession extends PsqlSession implements IWriteSession {

  private static final Logger log = LoggerFactory.getLogger(PsqlWriteSession.class);

  PsqlWriteSession(
      @NotNull PostgresStorage storage, @NotNull NakshaContext context, @NotNull PsqlConnection connection) {
    super(storage, context, connection);
  }

  @Override
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return session().lockFeature(collectionId, featureId, timeout, timeUnit);
  }

  @Override
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    return session().lockStorage(lockId, timeout, timeUnit);
  }

  @Override
  public void commit(boolean autoCloseCursors) {
    try {
      session().commit(autoCloseCursors);
    } catch (final SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void rollback(boolean autoCloseCursors) {
    try {
      session().rollback(autoCloseCursors);
    } catch (final SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void close(boolean autoCloseCursors) {
    session().close(autoCloseCursors);
  }

  @Override
  public @NotNull Result execute(@NotNull ReadRequest<?> readRequest) {
    return session().executeRead(readRequest);
  }

  @Override
  public @NotNull Result execute(@NotNull WriteRequest<?, ?, ?> writeRequest) {
    return session().executeWrite(writeRequest);
  }

  @Override
  public @NotNull Result process(@NotNull Notification<?> notification) {
    return session().process(notification);
  }
}
